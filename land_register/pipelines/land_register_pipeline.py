# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from collections import OrderedDict
from land_register import db_handler
from land_register.pipelines import common
from pprint import pprint


class LandRegisterPipeline():
    def __init__(self):
        self.collection = OrderedDict()

    def open_spider(self, spider):
        self.collection = OrderedDict()

    def close_spider(self, spider):
        self.save_all_items()

    def process_item(self, item, spider):
        cislo_lv = item.get('cislo_lv')

        if cislo_lv not in self.collection:
            self.init_item(cislo_lv)

        self.add_item(cislo_lv, item, spider)
        self.save_complete_items_in_order()

        return item

    def init_item(self, cislo_lv):
        self.collection[cislo_lv] = {
            'lv': {},
            'pozemky': [],
            'stavebni_objekty': [],
            'stavebni_objekty_ref': [],
            'stavby': [],
            'jednotky': []
        }

    def add_item(self, cislo_lv, item, spider=None):
        item_type = item.get('item_type')
        item_data = item.get('data')

        if spider:
            spider.logger.info('cislo_lv: {}, item: {}'.format(cislo_lv, item_type))

        format_item(item_data)

        if item_type == 'LV':
            self.collection[cislo_lv]['lv'] = item_data
        elif item_type == 'POZEMEK':
            self.collection[cislo_lv]['pozemky'].append(item_data)
        elif item_type == 'STAVEBNI_OBJEKT':
            self.collection[cislo_lv]['stavebni_objekty'].append(item_data)
        elif item_type == 'STAVEBNI_OBJEKT_REF':
            self.collection[cislo_lv]['stavebni_objekty_ref'].append(item_data)
        elif item_type == 'STAVBA':
            self.collection[cislo_lv]['stavby'].append(item_data)
        elif item_type == 'JEDNOTKA':
            self.collection[cislo_lv]['jednotky'].append(item_data)

    def save_gradually(self, cislo_lv):
        """Saves only some items, based on gradual processing.
        Parts of items can be processed in different order (parallel
        processing). It means that whole item may not be loaded immadiately.
        So item will be processed after some items have been loaded already."""

        to_lv = cislo_lv - 50
        from_lv = to_lv - 10
        if from_lv < 0:
            from_lv = 1

        for lv_candidate in range(from_lv, to_lv + 1):
            if lv_candidate in self.collection:
                item = self.collection.pop(lv_candidate)
                save_whole_item(item)

    def save_complete_items_in_order(self):
        """Saves only fully fetched items (some other item is processing right now)."""

        if len(self.collection) <= 1:
            return

        while len(self.collection) > 1:
            _, item = self.collection.popitem(last=False)
            save_whole_item(item)

    def save_all_items(self):
        """Saves all scraped items."""

        for _, item in self.collection.items():
            save_whole_item(item)


def format_item(item):
    """Format item values to types for DB."""

    for key, value in item.items():
        if key in [
            'cislo_lv', 'cislo_ku', 'cislo_obce', 'cislo_casti_obce',
            'vymera', 'cislo_stavebniho_objektu', 'ext_id_parcely',
            'pocet_bytu', 'zastavena_plocha', 'podlahova_plocha',
            'pocet_podlazi', 'ext_id_stavebniho_objektu'
        ]:
            if value:
                item[key] = common.string_to_int(value)

        elif key in ['datum_dokonceni']:
            if value:
                item[key] = common.get_date_from_string(value)


def save_whole_item(item):
    """Saves all parts of item separately."""

    id_lv = process_lv(item['lv'])
    if item['pozemky']:
        process_items_list(id_lv, 'pozemek', item['pozemky'])
    if item['stavebni_objekty']:
        process_items_list(id_lv, 'stavebni_objekt', item['stavebni_objekty'])
    if item['stavebni_objekty_ref']:
        save_buildings_refs(id_lv, item['stavebni_objekty_ref'])
    if item['stavby']:
        process_items_list(id_lv, 'stavba', item['stavby'])
    if item['jednotky']:
        process_items_list(id_lv, 'jednotka', item['jednotky'])


def process_lv(new_item):
    """Process and save LV."""

    filter_lv_change(new_item)
    save_items('lv', [new_item])

    id_lv = new_item['id']
    process_owners(id_lv, 'lv', [new_item])

    return id_lv


def filter_lv_change(new_item):
    """Check for item change and filter (remove) unchanged fields."""

    primary_key = get_item_primary_key_name('lv')
    search_key = get_item_search_key('lv', new_item)

    db = db_handler.get_dataset()
    all_items = db['lv'].find(**search_key, order_by='cislo_zaznamu')

    current_item = {}
    for item in all_items:
        current_item.update((k,v) for k,v in item.items() if v is not None)

    # item is new => no changes have been made
    if not current_item:
        return

    get_changed_fields(current_item, new_item)

    new_item[primary_key] = current_item[primary_key]

    if is_changed(new_item, ignore_fields=['vlastnici', primary_key]):
        new_item['cislo_zaznamu'] = current_item['cislo_zaznamu'] + 1
    else:
        new_item['zadna_zmena'] = True


def process_owners(id_lv, table_name, items):
    """Process and save owners."""

    primary_key = get_item_primary_key_name(table_name)

    for item in items:
        owners = item.get('vlastnici', [])
        for o in owners:
            o['id_ref'] = item[primary_key]
            o['typ_ref'] = table_name

        if not owners:
            continue

        additional_keys = {
            'id_ref': item[primary_key],
            'typ_ref': table_name
        }

        filter_items_changes(id_lv, 'vlastnici', owners, additional_keys)
        save_items('vlastnici', owners, id_lv)


def process_items_list(id_lv, table_name, items):
    """Process list of items and save them.
    Save additional items too - owners, ..."""

    # this should never happen - just for instance
    # if some data were accidentally fetched multiple times, filter unique items
    items = delete_duplicates(items)
    filter_items_changes(id_lv, table_name, items)
    save_items(table_name, items, id_lv)
    process_owners(id_lv, table_name, items)


def filter_items_changes(id_lv, table_name, new_items, additional_keys={}):
    """Filters items: compare items with current records in DB,
    if record already exists, return only changed fields."""

    # get name of primary key for futher processing (it should be only one)
    primary_key = get_item_primary_key_name(table_name)

    # first, load all current items
    current_items = get_current_items(id_lv, table_name, additional_keys)

    # now, try to match new and current items based on search key
    for current_item in current_items:
        # search key is (or should be) unique for item
        search_key = get_item_search_key(table_name, current_item)
        # if current item is not in new items list, write deleted flag
        is_deleted = True
        for new_item in new_items:
            # check if search key of current item if subset of new item
            if not (search_key.items() <= new_item.items()):
                continue

            # match found! item has not been deleted
            is_deleted = False

            # now, get only changed fields for new item
            get_changed_fields(current_item, new_item)

            # write primary key for new item.
            # it has to be done after comparing changing fields!
            new_item[primary_key] = current_item[primary_key]

            # if there are any changes, prepare item for save,
            # otherwise mark item to not save
            if is_changed(new_item, ignore_fields=['vlastnici', primary_key]):
                new_item['cislo_zaznamu'] = current_item['cislo_zaznamu'] + 1
            else:
                new_item['zadna_zmena'] = True

        # if item has been deleted, create new item with delete flag
        if is_deleted:
            new_items.append({
                primary_key: current_item[primary_key],
                'cislo_zaznamu': current_item['cislo_zaznamu'] + 1,
                'bylo_vymazano': True
            })


def save_items(table_name, items, id_lv=None):
    """Save items into DB. Items are saved separately, because we want
    to store id for futher processing."""

    primary_key = get_item_primary_key_name(table_name)

    for item in items:
        is_change = not (item.get('zadna_zmena', False))
        if is_change:
            # exclude owners from saving item, we add it after save
            owners = item.pop('vlastnici', [])

            if 'cislo_zaznamu' not in item:
                item['cislo_zaznamu'] = 1

            if id_lv:
                item['id_lv'] = id_lv

            db = db_handler.get_dataset()
            try:
                item[primary_key] = db[table_name].insert(item)
            except Exception as e:
                pprint('Exception for item: {}\n\n'.format(item))
                pprint(e)

            item['vlastnici'] = owners


def get_current_items(id_lv, table_name, additional_fields={}):
    """Loads current items for single LV."""

    all_items = load_items(id_lv, table_name, additional_fields)
    primary_key = get_item_primary_key_name(table_name)

    current_items = {}
    for item in all_items:
        item_key = item[primary_key]
        if item_key in current_items:
            current_items[item_key].update((k, v)
                for k, v in item.items() if v is not None)
        else:
            current_items[item_key] = item

    return list(current_items.values())


def load_items(id_lv, table_name, additional_fields={}):
    """Loads all items (include historical changes) for single LV."""

    db = db_handler.get_dataset()
    return db[table_name].find(
        id_lv=id_lv,
        **additional_fields,
        order_by='cislo_zaznamu'
    )


def get_item_search_key(table_name, item):
    """Returns search key of item, based on table name. It's not a primary
    key of item, but it should be a unique set of fields."""

    fields = None
    if table_name == 'lv':
        fields = ('cislo_ku', 'cislo_lv')

    elif table_name == 'pozemek':
        fields = ('ext_id_parcely',)

    elif table_name == 'stavebni_objekt':
        fields = ('ext_id_parcely', 'cisla_popis_evid')

    elif table_name == 'stavba':
        fields = ('stoji_na_pozemku', 'ext_id_stavebniho_objektu')

    elif table_name == 'jednotka':
        fields = ('cislo_jednotky',)

    elif table_name == 'vlastnici':
        fields = ('vlastnicke_pravo',)

    if not fields:
        return

    search_key = {k: item.get(k) for k in fields}
    return search_key


def get_item_primary_key_name(table_name):
    """Get primary key of table."""

    key = 'id'
    if table_name == 'pozemek':
        key = 'ext_id_parcely'
    return key


def get_changed_fields(old_item, new_item):
    """Get only fields that have been changed, otherwise None."""

    for key, value in old_item.items():
        # new value may not exist, it can be key or seqnr
        new_value = new_item.get(key)
        if value == new_value:
            new_item[key] = None


def is_changed(item, ignore_fields=[]):
    """Check for non None values in item, if exist return True,
    otherwise False."""

    for key, value in item.items():
        if key in ignore_fields:
            continue
        if value:
            return True
    return False


def save_buildings_refs(id_lv, refs):
    refs = delete_duplicates(refs)
    db = db_handler.get_dataset()
    for r in refs:
        r['id_lv'] = id_lv
    db['stavebni_objekt_ref'].insert_many(refs)


def delete_duplicates(items):
    return [dict(s) for s in set(frozenset(d.items()) for d in items)]
