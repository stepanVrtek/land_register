from datetime import datetime


def get_date_from_string(string):
    return datetime.strptime(string, '%d.%m.%Y')


def get_datetime_from_string(string):
    return datetime.strptime(string, '%d.%m.%Y %H:%M')


def string_to_int(value):
    if isinstance(value, str):
        # get only digits from string
        return int(''.join(filter(lambda x: x.isdigit(), value)))
    return value
