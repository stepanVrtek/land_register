# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import mysql.connector as mariadb
from datetime import datetime


class LandRegisterPipeline(object):
    def process_item(self, item, spider):
        return item



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
        type = item.get('item_type')

        if type == 'LOG':
            process_log(item)

        if type == 'LV':
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


def process_log(item):
    query = """INSERT INTO log_lv(cislo_lv, cislo_ku, existuje)
                 VALUES (%s, %s, %s)"""
    values = (
        item.get('cislo_lv'),
        item.get('cislo_ku'),
        item.get('existuje')
    )
    insert_or_update(query, values)

def process_lv(item):
    update_lv(item)
    id_lv = load_id_lv(item)

    type = item.get('item_type')
    vlastnici = item.get('vlastnici')
    process_vlastnici(vlastnici, id_lv, id_lv, type)

def process_pozemek(item):
    id_lv = get_id_lv(item)
    ext_id_parcely = item.get('ext_id_parcely')

    query = """INSERT INTO pozemek(ext_id_parcely, id_lv, parcelni_cislo,
                obec, cislo_obce, vymera, typ_parcely, druh_pozemku,
                cislo_stavebniho_objektu, zpusob_ochrany_nemovitosti,
                omezeni_vlastnickeho_prava)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    values = (
        ext_id_parcely,
        id_lv,
        item.get('parcelni_cislo'),
        item.get('obec'),
        item.get('cislo_obce'),
        item.get('vymera'),
        item.get('typ_parcely'),
        item.get('druh_pozemku'),
        item.get('cislo_stavebniho_objektu'),
        item.get('zpusob_ochrany_nemovitosti'),
        item.get('omezeni_vlastnickeho_prava')
    )
    insert_or_update(query, values)

    type = item.get('item_type')

    if item.get('vlastnici'):
        process_vlastnici(item.get('vlastnici'), id_lv, ext_id_parcely, type)

    # if item.get('rizeni'):
    #     process_seznam_rizeni(item.get('rizeni'), id_lv, ext_id_parcely, type)

def process_stavebni_objekt(item):
    query = """INSERT INTO stavebni_objekt(ext_id_parcely,
                cisla_popis_evid, typ, zpusob_vyuziti, datum_dokonceni,
                pocet_bytu, zastavena_plocha, podlahova_plocha,
                pocet_podlazi, ext_id_stavebniho_objektu)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    datum = item.get('datum_dokonceni')
    datum_dokonceni = format_date(datum) if datum else datum

    pocet_bytu = string_to_int(item.get('pocet_bytu'))
    zastavena_plocha = string_to_int(item.get('zastavena_plocha'))
    podlahova_plocha = string_to_int(item.get('podlahova_plocha'))
    pocet_podlazi = string_to_int(item.get('pocet_podlazi'))

    values = (
        item.get('ext_id_parcely'),
        item.get('cisla_popis_evid'),
        item.get('typ'),
        item.get('zpusob_vyuziti'),
        datum_dokonceni,
        pocet_bytu,
        zastavena_plocha,
        podlahova_plocha,
        pocet_podlazi,
        item.get('ext_id_stavebniho_objektu')
    )
    insert_or_update(query, values)

def process_stavba(item):
    id_lv = get_id_lv(item)

    query = """INSERT INTO stavba(id_lv, obec, cislo_obce,
                cast_obce, cislo_casti_obce, typ_stavby,
                zpusob_vyuziti, ext_id_stavebniho_objektu)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    values = (
        id_lv,
        item.get('obec'),
        item.get('cislo_obce'),
        item.get('cast_obce'),
        item.get('cislo_casti_obce'),
        item.get('typ_stavby'),
        item.get('zpusob_vyuziti'),
        item.get('ext_id_stavebniho_objektu')
    )
    id_stavby = insert_or_update(query, values)

    type = item.get('item_type')

    if item.get('vlastnici'):
        process_vlastnici(item.get('vlastnici'), id_lv, id_stavby, type)

    # if item.get('rizeni'):
    #     process_seznam_rizeni(item.get('rizeni'), id_lv, id_stavby, type)

def process_jednotka(item):
    id_lv = get_id_lv(item)

    query = """INSERT INTO jednotka(id_lv, cislo_jednotky, typ_jednotky,
                zpusob_vyuziti, podil_na_spol_castech, zpusob_ochrany_nemovitosti,
                omezeni_vlastnickeho_prava, jine_zapisy)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    values = (
        id_lv,
        item.get('cislo_jednotky'),
        item.get('typ_jednotky'),
        item.get('zpusob_vyuziti'),
        item.get('podil_na_spol_castech'),
        item.get('zpusob_ochrany_nemovitosti'),
        item.get('omezeni_vlastnickeho_prava'),
        item.get('jine_zapisy')
    )
    id_jednotky = insert_or_update(query, values)

    type = item.get('item_type')

    if item.get('vlastnici'):
        process_vlastnici(item.get('vlastnici'), id_lv, id_jednotky, type)

    # if item.get('rizeni'):
    #     process_seznam_rizeni(item.get('rizeni'), id_lv, id_jednotky, type)


def process_vlastnici(vlastnici, id_lv, id_ref, typ_ref):
    if not vlastnici:
        return

    values_list = []
    for v in vlastnici:
        values_list.append((
            id_lv,
            id_ref,
            typ_ref,
            v.get('vlastnicke_pravo'),
            v.get('jmeno'),
            v.get('adresa'),
            v.get('podil')
        ))

    values = ', '.join(map(str, values_list)).replace('None', "'NULL'")
    query = """INSERT INTO vlastnici(id_lv, id_ref, typ_ref,
                vlastnicke_pravo, jmeno, adresa, podil)
                VALUES {}""".format(values)

    insert_or_update(query)

# def process_seznam_rizeni(rizeni, id_lv, id_ref, typ_ref):
#     if not rizeni:
#         return
#
#     values_list = []
#     for r in rizeni:
#         values_list.append((
#             r.get('cislo_rizeni'),
#             id_lv,
#             id_ref,
#             typ_ref
#         ))
#
#     values = ', '.join(map(str, values_list)).replace('None', "'NULL'")
#     query = """INSERT INTO rizeni(cislo_rizeni, id_lv, id_ref, typ_ref)
#                 VALUES {}
#                 ON DUPLICATE KEY UPDATE
#                     id_lv = VALUES(id_lv),
#                     id_ref = VALUES(id_ref),
#                     typ_ref = VALUES(typ_ref)""".format(values)
#
#     insert_or_update(query)



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

def get_unique_elements(t1, t2, num_of_ignored_pairs=0):
    if len(t1) != len(t2):
        print('Rozdielne dlzky zaznamov')

    length = len(t1)

    for i in range(length):
        if t1[i] == t2[i]:
            t2[i] = None

def is_change(t1, t2):
    return any(t1[i] != t2[i] for i in range(len(t1)))
