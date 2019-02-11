from unittest.mock import MagicMock
from datetime import datetime, date
from pprint import pprint
import land_register.crawlers.operations_crawler as crawler

crawler.get_last_process = MagicMock(
    return_value={
        'pracoviste': 810,
        'typ': 'Z',
        'datum': date(2019, 2, 3)
    })

def test_get_next():
    pprint(crawler.get_next_batch(10))


if __name__ == '__main__':
    test_get_next()
