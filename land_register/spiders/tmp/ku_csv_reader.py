import csv
from datetime import datetime

import land_register.db_handler as db_handler


def save_ku_csv_into_db(file):
    """Saves all KU items into DB."""

    with open(file, mode='r', encoding='utf8') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        items_to_save = []
        for row in csv_reader:
            item = prepare_ku_to_save(row)
            items_to_save.append(item)
        
    db = db_handler.get_dataset()
    db['ku'].insert_many(items_to_save)


def prepare_ku_to_save(item):
    """Prepare KU to save."""

    item_to_save = {
        'cislo_ku': item.get('KOD'),
        'nazev_ku': item.get('NAZEV'),
        'cislo_obce': item.get('OBEC_KOD')
    }

    date_from = item.get('PLATI_OD')
    item_to_save['plati_od'] = format_date(date_from) if date_from else None

    date_to = item.get('PLATI_DO')
    item_to_save['plati_do'] = format_date(date_to) if date_to else None

    return item_to_save


def delete_all_ku():
    """Deletes all ku data."""

    db = db_handler.get_dataset()
    db['ku'].delete()


def format_date(string):
    """Format date from 'DD.MM.YYYY HH:MM:SS'"""

    return datetime.strptime(string, '%d.%m.%Y %H:%M:%S')


KU_CSV = 'land_register/UI_KATASTRALNI_UZEMI.csv'

if __name__ == '__main__':
    delete_all_ku()
    save_ku_csv_into_db(KU_CSV)
