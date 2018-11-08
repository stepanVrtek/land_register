import csv

def get_ku_codes(file):
    with open(file, mode='r', encoding='utf8') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        items = []
        for row in csv_reader:
            items.append(row["KOD"])
            
        return items


# codes = get_ku_codes('UI_KATASTRALNI_UZEMI.csv')
# for code in codes:
#     print(code)
