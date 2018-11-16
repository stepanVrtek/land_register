# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from land_register import db_handler
import common
from pprint import pprint

class OperationsPipeline():
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
    item['datum_prijeti'] = common.get_datetime_from_string(
                                datum) if datum else datum

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
        p['datum'] = common.get_date_from_string(datum) if datum else datum

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
