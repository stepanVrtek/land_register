# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import mysql.connector as mariadb
import land_register.db_handler as db_handler
import dataset
from datetime import datetime
from pprint import pprint


class CollectionPipeline():
    def __init__(self):
        self.collection = {}

    def open_spider(self, spider):
        self.collection = {}

    def close_spider(self, spider):
        save_all_items()

    def process_item(self, item, spider):
        cislo_lv = (item['cislo_lv'] if 'cislo_lv' in item
                    else item.get('data', {}).get('cislo_lv'))

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
        item_type = item.get('type')
        item_data = item.get('data')

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

    def save_gradually(self, cislo_lv):
        """Saves only some items, based on gradual processing.
        Parts of items can be processed in different order (parallel
        processing). It means that whole item may not be loaded immadiately.
        So item will be processed after some items have been loaded already."""

        from_lv = cislo_lv - 20
        to_lv = cislo_lv
        if from_lv < 0:
            from_lv = 1

        for i in range(from_lv, to_lv):
            if lv_candidate in self.collection:
                item = self.collection.pop(lv_candidate)
                save_whole_item(item)

    def save_all_items(self):
        for _, item in self.collection:
            save_whole_item(item)


def save_whole_item(item):
    """Saves all parts of item separately."""

    lv = item['lv']
    id_lv = save_single_item('lv', lv)

    save_grounds(id_lv, item['pozemky']):

    for so in item['stavebni_objekty']:
        save_item('stavebni_objekt', so)

    for s in item['stavby']:
        save_item('stavba', s)

    for j in item['jednotky']:
        save_item('jednotka', j)







class LandRegisterPipeline(object):
    def process_item(self, item, spider):
        return item



def get_dataset():
    # testing
    return dataset.connect(
        'mysql://user:password@localhost/katastr_db')


def get_connection():
    return mariadb.connect(
        # host='katastr-db.csnbslf6zcko.eu-central-1.rds.amazonaws.com',
        # user='devmons',
        # password='NG1MMUGuZBgT7rxvnpYq',
        user='user',
        password='password',
        database='katastr_db')


def insert_or_update(query, values=None):
    id = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        connection.commit()

        id = cursor.lastrowid
    except Exception as e:
         print(e)
    finally:
        cursor.close()
        connection.close()

    return id


def load_one(query, values):
    result = None
    try:
        connection = get_connection()
        cursor = connection.cursor(buffered=True)

        cursor.execute(query, values)
        result = cursor.fetchone()
        connection.commit()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()

    return result


def insert_lv(lv_item):
    query = """INSERT INTO lv(cislo_lv, cislo_ku, prava_stavby)
                VALUES (%s, %s, %s)"""
    values = (
        lv_item.get('cislo_lv'),
        lv_item.get('cislo_ku'),
        lv_item.get('prava_stavby')
    )
    insert_or_update(query, values)


def update_lv(lv_item):
    query = """INSERT INTO lv(cislo_lv, cislo_ku, prava_stavby)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    cislo_lv = VALUES(cislo_lv),
                    cislo_ku = VALUES(cislo_ku),
                    prava_stavby = VALUES(prava_stavby)"""
    values = (
        lv_item.get('cislo_lv'),
        lv_item.get('cislo_ku'),
        lv_item.get('prava_stavby')
    )
    insert_or_update(query, values)


def load_id_lv(lv_item):
    query = """SELECT id FROM lv
                WHERE cislo_lv = %s AND
                      cislo_ku = %s"""
    values = (
        lv_item.get('cislo_lv'),
        lv_item.get('cislo_ku')
    )

    result = load_one(query, values)
    if result:
        return result[0]
    else:
        print('LV {} na KU {} v DB neexistuje!'.format(
            lv_item.get('cislo_lv'), lv_item.get('cislo_ku')))
        return None


def get_id_lv(item):
    lv_item = item.get('lv_item')
    id_lv = load_id_lv(lv_item)
    if id_lv is None:
        update_lv(lv_item)
        id_lv = load_id_lv(lv_item)

    return id_lv


def load_id_rizeni(item):
    query = """SELECT id FROM rizeni
                WHERE cislo_rizeni = %s AND
                      cislo_pracoviste = %s"""
    values = (
        item.get('cislo_rizeni'),
        item.get('cislo_pracoviste')
    )

    result = load_one(query, values)
    if result:
        return result[0]
    else:
        print('ID pre cislo rizeni {} na KP {} v DB neexistuje!'.format(
            item.get('cislo_rizeni'), item.get('cislo_pracoviste')))
        return None



class SQLPipeline():
    def process_item(self, item, spider):
        pprint(item)
        return

        type = item.get('item_type')

        if type == 'LOG_LV':
            process_log_lv(item)
        elif type == 'LOG_ULOHY':
            process_log_ulohy(item)

        elif type == 'LV':
            process_lv(item)
        elif type == 'POZEMEK':
            process_pozemek(item)
        elif type == 'STAVEBNI_OBJEKT':
            process_stavebni_objekt(item)
        elif type == 'STAVBA':
            process_stavba(item)
        elif type == 'JEDNOTKA':
            process_jednotka(item)

        elif type == 'RIZENI':
            process_rizeni(item)
        elif type == 'REF_JEDNOTKA_RIZENI':
            process_ref_jednotka_rizeni(item)
        elif type == 'REF_PARCELA_RIZENI':
            process_ref_parcela_rizeni(item)
        else:
            pass


def process_log_lv(item):
    query = """INSERT INTO log_lv(cislo_lv, cislo_ku, existuje)
                 VALUES (%s, %s, %s)"""
    values = (
        item.get('cislo_lv'),
        item.get('cislo_ku'),
        item.get('existuje')
    )
    insert_or_update(query, values)


def process_log_ulohy(item):
    query = """UPDATE log_uloha SET dokonceno = %s
                WHERE id_ulohy = %s"""
    values = (
        item.get('dokonceno'),
        item.get('id_ulohy')
    )
    insert_or_update(query, values)


def process_lv(item):
    update_lv(item)
    id_lv = load_id_lv(item)

    type = item.get('item_type')
    vlastnici = item.get('vlastnici')
    if vlastnici:
        process_vlastnici(vlastnici, id_lv, id_lv, type)


def process_pozemek(item):
    id_lv = get_id_lv(item)

    # save item
    new_data = item.get('pozemek')
    new_data['id_lv'] = id_lv
    save_item('pozemek', new_data)

    # save owners
    ext_id_parcely = item.get('ext_id_parcely')
    type = item.get('item_type')
    vlastnici = item.get('vlastnici')
    if vlastnici:
        process_vlastnici(vlastnici, id_lv, ext_id_parcely, type)

    # # save operations
    # rizeni = item.get('rizeni')
    # if rizeni:
    #     process_seznam_rizeni(rizeni, id_lv, ext_id_parcely, type)


def process_stavebni_objekt(item):
    id_lv = get_id_lv(item)

    # prepare item and save
    new_data = item.get('stavebni_objekt')
    new_data['id_lv'] = id_lv

    datum = new_data.get('datum_dokonceni')
    new_data['datum_dokonceni'] = format_date(datum) if datum else datum
    new_data['pocet_bytu'] = string_to_int(new_data.get('pocet_bytu'))
    new_data['zastavena_plocha'] = string_to_int(new_data.get('zastavena_plocha'))
    new_data['podlahova_plocha'] = string_to_int(new_data.get('podlahova_plocha'))
    new_data['pocet_podlazi'] = string_to_int(new_data.get('pocet_podlazi'))

    save_item('stavebni_objekt', new_data)


def process_stavba(item):
    id_lv = get_id_lv(item)

    # save item
    new_data = item.get('stavba')
    new_data['id_lv'] = id_lv
    id_stavby = save_item('stavba', new_data)

    # save owners
    type = item.get('item_type')
    vlastnici = item.get('vlastnici')
    if vlastnici:
        process_vlastnici(vlastnici, id_lv, id_stavby, type)

    # # save operation
    # rizeni = item.get('rizeni')
    # if rizeni:
    #     process_seznam_rizeni(rizeni, id_lv, id_stavby, type)

def process_jednotka(item):
    id_lv = get_id_lv(item)

    # save item
    new_data = item.get('jednotka')
    new_data['id_lv'] = id_lv
    id_jednotky = save_item('jednotka', new_data)

    # save owners
    type = item.get('item_type')
    vlastnici = item.get('vlastnici')
    if vlastnici:
        process_vlastnici(vlastnici, id_lv, id_jednotky, type)

    # # save operation
    # rizeni = item.get('rizeni')
    # if rizeni:
    #     process_seznam_rizeni(rizeni, id_lv, id_jednotky, type)


def process_vlastnici(vlastnici, id_lv, id_ref, typ_ref):
    for v in vlastnici:
        v['id_lv'] = id_lv
        v['id_ref'] = id_ref
        v['typ_ref'] = typ_ref

    values = ', '.join(map(str, values_list)).replace('None', "'NULL'")
    query = """INSERT INTO vlastnici(id_lv, id_ref, typ_ref,
                vlastnicke_pravo, jmeno, adresa, podil)
                VALUES {}""".format(values)

    insert_or_update(query)


############## ŘÍZENÍ ##############

def process_rizeni(item):
    query = """INSERT INTO rizeni(cislo_pracoviste, cislo_rizeni,
                cislo_ku, datum_prijeti, stav_rizeni)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    cislo_pracoviste = VALUES(cislo_pracoviste),
                    cislo_rizeni = VALUES(cislo_rizeni),
                    cislo_ku = VALUES(cislo_ku),
                    datum_prijeti = VALUES(datum_prijeti),
                    stav_rizeni = VALUES(stav_rizeni)"""

    datum = item.get('datum_prijeti')
    datum_prijeti = format_datetime(datum) if datum else datum

    values = (
        item.get('cislo_pracoviste'),
        item.get('cislo_rizeni'),
        item.get('cislo_ku'),
        datum_prijeti,
        item.get('stav_rizeni')
    )
    id_rizeni = insert_or_update(query, values)

    if item.get('ucastnici'):
        process_ucastnici_rizeni(item['ucastnici'], id_rizeni)
    if item.get('provedene_operace'):
        process_provedene_operace(item['provedene_operace'], id_rizeni)
    if item.get('predmety_rizeni'):
        process_predmety_rizeni(item['predmety_rizeni'], id_rizeni)
    if item.get('seznam_nemovitosti'):
        process_seznam_nemovitosti(item['seznam_nemovitosti'], id_rizeni)


def process_ucastnici_rizeni(ucastnici, id_rizeni):
    # execute_query("""DELETE FROM ucastnici_rizeni
    #                 WHERE cislo_rizeni = {}""".format(cislo_rizeni))

    values_list = []
    for u in ucastnici:
        values_list.append((
            id_rizeni,
            u.get('poradove_cislo'),
            u.get('jmeno'),
            u.get('typ')
        ))

    values = ', '.join(map(str, values_list)).replace('None', "'NULL'")
    query = """INSERT INTO ucastnici_rizeni(id_rizeni,
                poradove_cislo, jmeno, typ)
                VALUES {}
                ON DUPLICATE KEY UPDATE
                    poradove_cislo = VALUES(poradove_cislo),
                    jmeno = VALUES(jmeno),
                    typ = VALUES(typ)""".format(values)

    insert_or_update(query)

def process_provedene_operace(provedene_operace, id_rizeni):
    # execute_query("""DELETE FROM provedene_operace
    #                 WHERE cislo_rizeni = {}""".format(cislo_rizeni))

    values_list = []
    for p in provedene_operace:

        datum = p.get('datum')
        datum = format_date(datum) if datum else datum

        values_list.append((
            id_rizeni,
            p.get('poradove_cislo'),
            p.get('operace'),
            datum
        ))

    values = ', '.join(map(str, values_list)).replace('None', "'NULL'")
    query = """INSERT INTO provedene_operace(id_rizeni,
                poradove_cislo, operace, datum)
                VALUES {}
                ON DUPLICATE KEY UPDATE
                    poradove_cislo = VALUES(poradove_cislo),
                    operace = VALUES(operace),
                    datum = VALUES(datum)""".format(values)

    insert_or_update(query)

def process_predmety_rizeni(predmety_rizeni, id_rizeni):
    # execute_query("""DELETE FROM predmety_rizeni
    #                 WHERE cislo_rizeni = {}""".format(cislo_rizeni))

    values_list = []
    for p in predmety_rizeni:

        values_list.append((
            id_rizeni,
            p.get('poradove_cislo'),
            p.get('typ')
        ))

    values = ', '.join(map(str, values_list)).replace('None', "'NULL'")
    query = """INSERT INTO predmety_rizeni(id_rizeni,
                poradove_cislo, typ)
                VALUES {}
                ON DUPLICATE KEY UPDATE
                    poradove_cislo = VALUES(poradove_cislo),
                    typ = VALUES(typ)""".format(values)

    insert_or_update(query)

def process_seznam_nemovitosti(seznam_nemovitosti, id_rizeni):
    # execute_query("""DELETE FROM seznam_nemovitosti
    #                 WHERE cislo_rizeni = {}""".format(cislo_rizeni))

    values_list = []
    for s in seznam_nemovitosti:
        values_list.append((
            id_rizeni,
            s.get('poradove_cislo'),
            s.get('typ'),
            s.get('cislo')
        ))

    values = ', '.join(map(str, values_list)).replace('None', "'NULL'")
    query = """INSERT INTO seznam_nemovitosti(id_rizeni,
                poradove_cislo, typ, cislo)
                VALUES {}
                ON DUPLICATE KEY UPDATE
                    poradove_cislo = VALUES(poradove_cislo),
                    typ = VALUES(typ),
                    cislo = VALUES(cislo)""".format(values)

    insert_or_update(query)

def process_ref_parcela_rizeni(item):
    number = item.get('parcelni_cislo')
    process_ref_rizeni(item, number, 'PARCELA')

def process_ref_jednotka_rizeni(item):
    number = item.get('cislo_jednotky')
    process_ref_rizeni(item, number, 'JEDNOTKA')

def process_ref_rizeni(item, number, typ):
    id_lv = get_id_lv(item)
    id_rizeni = load_id_rizeni(item)

    if not id_rizeni:
        return

    query = """UPDATE seznam_nemovitosti SET id_lv = %s
                WHERE id_rizeni = %s,
                      typ = %s,
                      cislo = %s"""
    values = (
        id_lv,
        id_rizeni,
        typ,
        number
    )
    insert_or_update(query, values)



def format_date(date):
    date_obj = datetime.strptime(date, '%d.%m.%Y')
    return date_obj.strftime('%Y-%m-%d')

def format_datetime(date):
    date_obj = datetime.strptime(date, '%d.%m.%Y %H:%M')
    return date_obj.strftime('%Y-%m-%d %H:%M:%S')

def string_to_int(string):
    if isinstance(string, str):
        return int(string)
    return None



def save_item(table_name, new_item):
    """Save new item, but only if there are any changes
    compared to current item."""

    search_key = get_item_search_key(table_name, new_item)
    get_new_item_diff(table_name, new_item, search_key)

    # save item only if there were any changes
    if new_item:
        db = get_dataset()
        return db[table_name].insert(new_item)

    return None


def get_item_search_key(table_name, item):
    """Returns search key of item, based on table name. It's not a primary
    key of item, but it should be a unique set of fields."""

    fields = None
    if table_name == 'pozemek':
        fields = ('ext_id_parcely', 'id_lv')

    elif table_name == 'stavebni_objekt':
        fields = ('id_lv', 'ext_id_parcely', 'cisla_popis_evid')

    elif table_name == 'stavba':
        fields = ('id_lv', 'stoji_na_pozemku', 'ext_id_stavebniho_objektu')

    elif table_name == 'jednotka':
        fields = ('id_lv', 'cislo_jednotky')

    elif table_name == 'vlastnici':
        fields = ('id_lv', 'id_ref', 'typ_ref', 'cislo_vlastnika')

    if not fields:
        return

    search_key = {k: item.get(k) for k in fields}
    return search_key


def get_new_item_diff(table_name, new_item, search_key):
    """Get new item, but only fields that have been changed compared
    to the last item."""

    db = get_dataset()
    # first, get all items of this object
    all_items = db[table_name].find(search_key, order_by=poradove_cislo)
    # second, get current item (intersection of previous changes on item)
    current_item = get_current_item(all_items)
    # third, get only difference on new and current item
    get_changed_fields(current_item, new_item)

    # finally, if there is any change in item, return it, otherwise None
    if is_changed(new_item):
        return item
    return None


def get_current_item(all_items):
    """Use all historical items and returns items with current valid data.
    In newer versions are only fields that have been changed, so we have to
    mix items (do intersection) from the oldest to newest."""

    latest = {}
    for item in all_items:
        latest.update((k,v) for k,v in item.items() if v is not None)
    return latest


def get_changed_fields(old_item, new_item):
    """Get only fields that have been changed, otherwise None."""

    for key, value in old_item.items():
        # new value may not exist, it can be key or seqnr
        new_value = new_item.get(key)
        if value == new_value:
            new_item[key] = None


def is_changed(item):
    """Check for non None values in item, if exist return True,
    otherwise False."""

    for _, value in item.items():
        if value:
            return True
    return False
