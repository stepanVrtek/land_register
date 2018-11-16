from datetime import timedelta
from datetime import date as datetime_date

import utils
from land_register import db_handler
from scrapy.utils.project import get_project_settings


class OperationsCrawler():

    project_name = 'land_register'
    spider_name = 'OperationsSpider'
    workplaces = [
        20, 101, 301, 302, 303, 305, 306, 307, 308, 701, 731, 702, 703, 704,
        706, 735, 738, 712, 713, 402, 403, 409, 602, 604, 605, 607, 610, 501,
        532, 504, 505, 608, 801, 802, 803, 831, 804, 806, 807, 832, 835, 811,
        805, 709, 808, 809, 603, 606, 609, 611, 401, 404, 435, 406, 405, 407,
        408, 410, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212,
        231, 502, 503, 506, 507, 508, 531, 509, 510, 533, 601, 707, 741, 304,
        710, 746, 714, 740, 708, 711, 742, 737, 836, 810, 705
    ]
    operation_types = ['V', 'Z']

    @staticmethod
    def run():
        scrapyd = utils.get_scrapyd()
        batch = create_scraping_batch()

        for process in batch:
            job_id = save_operation_log(process, status='R')

            scrapyd.schedule(
                OperationsCrawler.project_name,
                OperationsCrawler.spider_name,
                workplace=process['pracoviste'],
                type=process['typ'],
                job_id=job_id,
                date=process['datum'].strftime('%d.%m.%Y')
            )



def create_scraping_batch():

    settings = get_project_settings()
    max_processes_in_batch = settings['MAX_RIZENI_IN_BATCH']

    running_processes = get_running_processes()
    processes_to_add = max_processes_in_batch - len(running_processes)

    return get_next_batch(processes_to_add)


def get_running_processes():

    db = db_handler.get_dataset()
    results = db['log_rizeni'].find(stav='R')
    return [r for r in results]


def get_next_batch(processes_to_add):

    from_index = 0
    to_index = 0

    last_process = get_last_process()
    all_processes = create_all_process_possibilities()

    if last_process:
        if last_process in all_processes:
            from_index = all_processes.index(last_process) + 1

    to_index = from_index + processes_to_add

    next_batch = all_processes[from_index:to_index]
    processes_to_add -= len(next_batch)

    if processes_to_add > 0:
        next_batch += all_processes[0:processes_to_add]

    return next_batch


def get_last_process():

    db = db_handler.get_dataset()
    result = db.query("""
        SELECT MAX(id) as id, pracoviste, typ, datum FROM log_rizeni"""
    )
    for r in result:
        if r.pop('id'):
            return r
    return None


def create_all_process_possibilities():

    settings = get_project_settings()
    days_to_past = settings['MAX_DAYS_IN_PAST_TO_SCRAPE_RIZENI']

    start_date = datetime_date.today()
    end_date = start_date - timedelta(days=days_to_past)
    date = start_date

    process_items = []
    while date >= end_date:
        for type in OperationsCrawler.operation_types:
            for wp in OperationsCrawler.workplaces:
                process_item = {
                    'datum': date,
                    'typ': type,
                    'pracoviste': wp
                }
                process_items.append(process_item)

        date -= timedelta(days=1)

    return process_items


def save_operation_log(process_item, status):

    process_item['stav'] = status
    db = db_handler.get_dataset()
    job_id = db['log_rizeni'].insert(process_item)
    return job_id



if __name__ == '__main__':
    OperationsCrawler.run()
