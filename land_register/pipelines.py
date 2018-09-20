# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
from scrapy.contrib.exporter import CsvItemExporter
import csv

import mysql.connector as mariadb


class LandRegisterPipeline(object):
    def process_item(self, item, spider):
        return item

def write_to_csv(item):
    # writer = csv.writer(open('analysis/export.csv', 'a'), lineterminator='\n')
    writer = csv.writer(open('analysis/storm_proxies5/KU_{}_export.csv'.format(item['cislo_ku']), 'a'), lineterminator='\n')
    writer.writerow([item[key] for key in item.keys()])

class WriteToCsv(object):
    def process_item(self, item, spider):
        write_to_csv(item)
        return item




def get_connection():
    return mariadb.connect(
        user='user',
        password='password',
        database='katastr')

def get_cursor(buffered=True):
    connection = get_connection()
    return connection.cursor(buffered=True)

def insert_values(query, values=None):
    try:
        connection = get_connection()
        cursor = connection.cursor()
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        connection.commit()

    except Exception as e:
         print(e)
    finally:
        cursor.close()
        connection.close()

def insert_lv(lv_item):
    query = """INSERT INTO lv(cislo_lv, cislo_ku, prava_stavby)
                VALUES (%s, %s, %s)"""
    values = (
        lv_item.get('cislo_lv'),
        lv_item.get('cislo_ku'),
        lv_item.get('prava_stavby')
    )
    insert_values(query, values)

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
    insert_values(query, values)

def load_id_lv(lv_item):
    query = """SELECT id FROM lv
                WHERE cislo_lv = %s AND
                      cislo_ku = %s"""
    values = (
        lv_item.get('cislo_lv'),
        lv_item.get('cislo_ku')
    )
    cursor = get_cursor()
    cursor.execute(query, values)

    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        print('LV {} na KU {} v DB neexistuje!'.format(
            lv_item.get('cislo_lv'), lv_item.get('cislo_ku')))
        return None

def get_id_lv(self, item):
    lv_item = item.get('lv_item')
    id_lv = load_id_lv(lv_item)
    if id_lv is None:
        update_lv(lv_item)
        id_lv = load_id_lv(lv_item)

    return id_lv

# def get_pozemek_id(id_lv, pozemek):
#     query = """SELECT id FROM pozemek
#                 WHERE id_lv = %s AND
#                       parcelni_cislo = %s"""
#     values = (
#         id_lv,
#         pozemek.get('parcelni_cislo')
#     )
#     cursor = get_cursor()
#     cursor.execute(query, values)
#
#     result = cursor.fetchone()
#     if result:
#         return result[0]
#     else:
#         print('Pozemok {} na LV s ID {} v DB neexistuje!'.format(
#             pozemek.get('parcelni_cislo'), id_lv))


class SQLPipeline():
    def process_item(self, item, spider):
        type = item.get('item_type')

        if type == 'lv':
            self.process_lv(item)
        elif type == 'pozemek':
            self.process_pozemek(item)
        elif type == 'stavebni_objekt':
            self.process_stavebni_objekt(item)
        elif type == 'stavba':
            self.process_stavba(item)
        elif type == 'jednotka':
            self.process_jednotka(item)
        elif type == 'rizeni':
            self.process_rizeni(item)
        elif type == 'ucastnici_rizeni':
            self.process_ucastnici_rizeni(item)
        elif type == 'provedene_operace_rizeni':
            self.process_provedene_operace_rizeni(item)
        elif type == 'seznam_nemovitosti_rizeni':
            self.process_seznam_nemovitosti_rizeni(item)
        else:
            pass

    def process_lv(self, item):
        update_lv(item)

    def process_pozemek(self, item):
        id_lv = get_id_lv(item)

        query = """INSERT INTO pozemek(id_parcely, id_lv, parcelni_cislo, obec,
                    cislo_obce, vymera, typ_parcely, druh_pozemku,
                    cislo_stavebniho_objektu, zpusob_ochrany_nemovitosti,
                    omezeni_vlastnickeho_prava)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        values = (
            item.get('id_parcely')
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
        insert_values(query, values)

    def process_stavebni_objekt(self, item):
        id_lv = get_id_lv(item)

        query = """INSERT INTO stavebni_objekt(id, id_lv, id_pozemku, cisla_popis_evid,
                    typ, zpusob_vyuziti, datum_dokonceni, pocet_bytu,
                    zastavena_plocha, podlahova_plocha, pocet_podlazi)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        values = (
            item.get('id_stavebniho_objektu'),
            id_lv,
            item.get('id_pozemku'),
            item.get('cisla_popis_evid'),
            item.get('typ'),
            item.get('zpusob_vyuziti'),
            item.get('datum_dokonceni'),
            item.get('pocet_bytu'),
            item.get('zastavena_plocha'),
            item.get('podlahova_plocha'),
            item.get('pocet_podlazi')
        )
        insert_values(query, values)

    def process_stavba(self, item):
        id_lv = get_id_lv(item)

        query = """INSERT INTO pozemek(id, id_lv, obec, cislo_obce, cast_obce,
                    cislo_casti_obce, typ_stavby, zpusob_vyuziti)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        values = (
            item.get('id_stavebniho_objektu'),
            id_lv,
            item.get('obec'),
            item.get('cislo_obce'),
            item.get('cast_obce'),
            item.get('cislo_casti_obce'),
            item.get('typ_stavby'),
            item.get('zpusob_vyuziti')
        )
        insert_values(query, values)

    def process_jednotka(self, item):
        id_lv = get_id_lv(item)

        query = """INSERT INTO jednotka(id_lv, cislo_jednotky, typ_jednotky,
                    zpusob_vyuziti, podil_na_spol_castech, zpusob_ochrany_nemovitosti,
                    omezeni_vlastnickeho_prava, jine_zapisy)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)"""
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
        insert_values(query, values)

    def process_rizeni(self, item):
        id_lv = get_id_lv(item)

        query = """INSERT INTO rizeni(cislo_rizeni, id_lv, id_ref,
                    typ_ref, datum_prijeti, stav_rizeni)
                    VALUES (%s, %s, %s, %s, %s, %s)"""
        values = (
            item.get('cislo_rizeni')
            id_lv,
            item.get('id_ref'),
            item.get('typ_ref'),
            item.get('datum_prijeti'),
            item.get('stav_rizeni')
        )
        insert_values(query, values)

    def process_ucastnici_rizeni(self, item):
        cislo_rizeni = item.get('cislo_rizeni')
        ucastnici = item.get('ucastnici')

        values_list = []
        for u in ucastnici:
            values_list.append((
                cislo_rizeni,
                u.get('poradove_cislo'),
                u.get('jmeno'),
                u.get('typ')
            ))

        values = ', '.join(map(str, values_list))
        query = """INSERT INTO ucastnici_rizeni(cislo_rizeni,
                    poradove_cislo, jmeno, typ)
                    VALUES {}""".format(values)

        insert_values(query)

    def process_provedene_operace_rizeni(self, item):
        cislo_rizeni = item.get('cislo_rizeni')
        provedene_operace = item.get('provedene_operace')

        values_list = []
        for p in provedene_operace:

            datum = p.get('datum')
            datum = format_date(datum) if datum else datum

            values_list.append((
                cislo_rizeni,
                p.get('poradove_cislo'),
                p.get('operace'),
                datum
            ))

        values = ', '.join(map(str, values_list))
        query = """INSERT INTO provedene_operace(cislo_rizeni,
                    poradove_cislo, operace, datum)
                    VALUES {}""".format(values)

        insert_values(query)

    def process_seznam_nemovitosti_rizeni(self, item):
        cislo_rizeni = item.get('cislo_rizeni')
        seznam_nemovitosti = item.get('seznam_nemovitosti')

        values_list = []
        for s in seznam_nemovitosti:
            values_list.append((
                cislo_rizeni,
                s.get('poradove_cislo'),
                s.get('typ'),
                s.get('cislo')
            ))

        values = ', '.join(map(str, values_list))
        query = """INSERT INTO seznam_nemovitosti(cislo_rizeni,
                    poradove_cislo, typ, cislo)
                    VALUES {}""".format(values)

        insert_values(query)

def format_date(date):
    date_obj = datetime.strptime(date, '%d-%m-%Y')
    return date_obj.strftime('%Y-%m-%d')
