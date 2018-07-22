import csv

def get_ku_codes(file):
    with open(file, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
                continue
            yield row["KOD"]
            line_count += 1


codes = get_ku_codes('UI_KATASTRALNI_UZEMI.csv')
for code in codes:
    print(code)
