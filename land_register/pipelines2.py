# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import mysql.connector as mariadb
from land_register import db_handler
import dataset
from datetime import datetime
from pprint import pprint


def get_dataset():
    return dataset.connect('mysql://user:password@localhost/katastr_db')


class CollectionPipeline():
    def __init__(self):
        self.collection = {}

    def open_spider(self, spider):
        self.collection = {}

    def close_spider(self, spider):
        self.save_all_items()

    def process_item(self, item, spider):
        cislo_lv = item.get('cislo_lv')

        if cislo_lv not in self.collection:
            self.init_item(cislo_lv)
        self.add_item(cislo_lv, item)

        self.save_gradually(cislo_lv)

        return item

    def init_item(self, cislo_lv):
        self.collection[cislo_lv] = {
            'lv': {},
            'pozemky': [],
            'stavebni_objekty': [],
            'stavby': [],
            'jednotky': []
        }

    def add_item(self, cislo_lv, item):
        item_type = item.get('item_type')
        item_data = item.get('data')

        self.format_item(item_data)

        if item_type == 'LV':
            self.collection[cislo_lv]['lv'] = item_data
        elif item_type == 'POZEMEK':
            self.collection[cislo_lv]['pozemky'].append(item_data)
        elif item_type == 'STAVEBNI_OBJEKT':
            self.collection[cislo_lv]['stavebni_objekty'].append(item_data)
        elif item_type == 'STAVBA':
            self.collection[cislo_lv]['stavby'].append(item_data)
        elif item_type == 'JEDNOTKA':
            self.collection[cislo_lv]['jednotky'].append(item_data)

    def format_item(self, item):
        """Format item values to types for DB."""

        for key, value in item.items():
            if key in [
                'cislo_lv', 'cislo_ku', 'cislo_obce', 'cislo_casti_obce',
                'vymera', 'cislo_stavebniho_objektu', 'ext_id_parcely',
                'pocet_bytu', 'zastavena_plocha', 'podlahova_plocha',
                'pocet_podlazi', 'ext_id_stavebniho_objektu'
            ]:
                item[key] = string_to_int(value)

            elif key in ['datum_dokonceni']:
                item[key] = get_date_from_string(value)

    def save_gradually(self, cislo_lv):
        """Saves only some items, based on gradual processing.
        Parts of items can be processed in different order (parallel
        processing). It means that whole item may not be loaded immadiately.
        So item will be processed after some items have been loaded already."""

        to_lv = cislo_lv - 20
        from_lv = to_lv - 10
        if from_lv < 0:
            from_lv = 1

        for lv_candidate in range(from_lv, to_lv + 1):
            if lv_candidate in self.collection:
                item = self.collection.pop(lv_candidate)
                save_whole_item(item)

    def save_all_items(self):
        """Saves all scraped items."""

        for _, item in self.collection.items():
            save_whole_item(item)



def save_whole_item(item):
    """Saves all parts of item separately."""

    id_lv = process_lv(item['lv'])
    process_items_list(id_lv, 'pozemek', item['pozemky'])
    process_items_list(id_lv, 'stavebni_objekt', item['stavebni_objekty'])
    process_items_list(id_lv, 'stavba', item['stavby'])
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

    db = get_dataset()
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
    pprint('table: {}'.format(table_name))
    pprint(items)

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
            new_items.append = {
                primary_key: current_item[primary_key],
                'cislo_zaznamu': current_item['cislo_zaznamu'] + 1,
                'bylo_vymazano': True
            }


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

            db = get_dataset()
            item[primary_key] = db[table_name].insert(item)

            item['vlastnici'] = owners


def get_current_items(id_lv, table_name, additional_fields={}):
    """Loads current items for single LV."""

    all_items = load_items(id_lv, table_name, additional_fields)
    primary_key = get_item_primary_key_name(table_name)

    current_items = {}
    for item in all_items:
        item_key = item[primary_key]
        if item_key in current_items:
            current_items[item_key].update((k,v)
                for k,v in item.items() if v is not None)
        else:
            current_items[item_key] = item

    return list(current_items.values())


def load_items(id_lv, table_name, additional_fields={}):
    """Loads all items (include historical changes) for single LV."""

    db = get_dataset()
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


################################################################################


class RizeniPipeline():
    def process_item(self, item, spider):

        type = item.get('item_type')

        if type == 'RIZENI':
            process_rizeni(item)
        elif type == 'REF_JEDNOTKA_RIZENI':
            process_ref_jednotka_rizeni(item)
        elif type == 'REF_PARCELA_RIZENI':
            process_ref_parcela_rizeni(item)


def process_rizeni(item):

    data = item.get('data')

    datum = data.get('datum_prijeti')
    item['datum_prijeti'] = get_datetime_from_string(datum) if datum else datum

    db = db_handler.get_dataset()
    id_rizeni = db['rizeni'].upsert(data, ['cislo_pracoviste', 'cislo_rizeni'])

    if item.get('ucastnici'):
        process_ucastnici_rizeni(item['ucastnici'], id_rizeni)
    if item.get('provedene_operace'):
        process_provedene_operace(item['provedene_operace'], id_rizeni)
    if item.get('predmety_rizeni'):
        process_predmety_rizeni(item['predmety_rizeni'], id_rizeni)
    if item.get('seznam_nemovitosti'):
        process_seznam_nemovitosti(item['seznam_nemovitosti'], id_rizeni)


def process_ucastnici_rizeni(ucastnici, id_rizeni):

    db = db_handler.get_dataset()

    for u in ucastnici:
        u['id_rizeni'] = id_rizeni

        db['ucastnici_rizeni'].upsert(u, ['id_rizeni', 'poradove_cislo'])


def process_provedene_operace(provedene_operace, id_rizeni):

    db = db_handler.get_dataset()

    for p in provedene_operace:
        p['id_rizeni'] = id_rizeni
        datum = p.get('datum')
        p['datum'] = get_date_from_string(datum) if datum else datum

        db['provedene_operace'].upsert(p, ['id_rizeni', 'poradove_cislo'])


def process_predmety_rizeni(predmety_rizeni, id_rizeni):

    db = db_handler.get_dataset()

    for p in predmety_rizeni:
        p['id_rizeni'] = id_rizeni

        db['predmety_rizeni'].upsert(p, ['id_rizeni', 'poradove_cislo'])


def process_seznam_nemovitosti(seznam_nemovitosti, id_rizeni):

    db = db_handler.get_dataset()

    for s in seznam_nemovitosti:
        s['id_rizeni'] = id_rizeni

        db['seznam_nemovitosti'].upsert(s, ['id_rizeni', 'poradove_cislo'])


def process_ref_parcela_rizeni(item):

    number = item.get('parcelni_cislo')
    process_ref_rizeni(item, number, 'PARCELA')


def process_ref_jednotka_rizeni(item):

    number = item.get('cislo_jednotky')
    process_ref_rizeni(item, number, 'JEDNOTKA')


def process_ref_rizeni(item, number, type):

    lv_item = item.get('lv_item')
    id_lv = get_id_lv(lv_item)
    id_rizeni = get_id_rizeni(item)

    if not id_rizeni:
        return

    db = db_handler.get_dataset()
    db['seznam_nemovitosti'].update({
        'id_lv': id_lv,
        'id_rizeni': id_rizeni,
        'typ': type,
        'cislo': number
    }, ['id_rizeni', 'typ', 'cislo'])


def get_id_lv(item):
    """Get LV's ID, if not exists, item will be saved."""

    db = db_handler.get_dataset()

    return db['lv'].upsert(dict(
        cislo_ku=item.get('cislo_ku'),
        cislo_lv=item.get('cislo_lv'),
        cislo_zaznamu=1),
        ['cislo_ku', 'cislo_lv']
    )


def get_id_rizeni(item):
    """Get ID of rizeni, if not exists, item will be saved."""

    db = db_handler.get_dataset()

    return db['rizeni'].upsert(dict(
        cislo_pracoviste=item.get('cislo_pracoviste'),
        cislo_rizeni=item.get('cislo_rizeni')),
        ['cislo_pracoviste', 'cislo_rizeni']
    )



def get_date_from_string(string):
    return datetime.strptime(string, '%d.%m.%Y')

def get_datetime_from_string(string):
    return datetime.strptime(string, '%d.%m.%Y %H:%M')

def string_to_int(value):
    if isinstance(value, str):
        return int(value)
    return value
