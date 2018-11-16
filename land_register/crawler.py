from datetime import datetime
from datetime import timedelta
from datetime import date as datetime_date

from land_register import db_handler

from scrapy.utils.project import get_project_settings


class LandRegisterCrawler():
    """Class for scraping all data in land register."""

    project_name = 'land_register'
    spider_name = 'LVSpider'

    @staticmethod
    def run():
        scraping_batch = ScrapingBatch()
        scraping_batch.create()

        # if not scraping_batch.batch_content:
        #     print('Vsetky procesy su vytazene. Dalsie KU nebudu pridane.')
        #     return
        # else:
        #     print('Bude pridanych {} novych KU pre spracovanie.'.format(
        #         len(scraping_batch.batch_content)))

        scrapyd = get_scrapyd()
        for process in scraping_batch.batch_content:
            job_hash = scrapyd.schedule(
                LandRegisterCrawler.project_name,
                LandRegisterCrawler.spider_name,
                ku_code = process['ku_code'],
                job_id = process['job_id'],
                starting_lv = process['starting_lv']
            )
            # set job hash of process and status (R - running)
            update_job_log(process['job_id'], job_hash, 'R')



class ScrapingBatch():
    """Batch scraping process."""

    def __init__(self):
        self.scraping_num = None
        self.batch_num = None
        self.batch_content = []
        self.batch_id = None

    def create(self):
        """ Create scraping batch - multiple scraping processes."""

        self.scraping_id = get_scraping_id()
        # if this is first call of scraping, we have to crate one first
        if not self.scraping_id:
            self.scraping_id = init_new_scraping()

        self.prepare_batch_content()
        # if batch content wasn't created, it means that all scraping data
        # have been scraped and we have to create next scraping and start from
        # the beginning
        if not self.batch_content:
            # close_scraping(self.scraping_id) # TODO
            self.scraping_id = init_new_scraping()
            self.prepare_batch_content()

    def prepare_batch_content(self):
        """Prepares data for scraping."""

        # check for free slots in next batch
        running_ku_jobs = get_ku_jobs(self.scraping_id, 'R')
        current_ku_in_batch = len(running_ku_jobs)
        settings = get_project_settings()
        max_ku_in_batch = settings['MAX_KU_IN_BATCH']
        ku_to_add = max_ku_in_batch - current_ku_in_batch

        # there is no empty slot for new ku, stop processing
        if ku_to_add <= 0:
            return

        # get number of processes in new batch
        error_ku_jobs = get_ku_jobs(self.scraping_id, 'E')
        # slice list of error jobs only for allowed number of ku
        error_ku_jobs = error_ku_jobs[:ku_to_add]
        # decrease number of allowed ku
        ku_to_add -= len(error_ku_jobs)

        # get list of new ku which are waiting to process
        # (only if there are still free slots)
        waiting_ku_codes = []
        if ku_to_add:
            waiting_ku_codes = get_ku_jobs(
                self.scraping_id, 'W', limit=ku_to_add)
            ku_to_add = 0

        # add error and new jobs to batch
        for ku_job in error_ku_jobs:
            last_processed_lv = get_last_processed_lv(ku_job['id_ulohy'])

            # last processed lv may not be downloaded completely, we have to
            # delete all the data which are connected with this lv
            # downloading will start from the beginning
            delete_whole_lv_item(last_processed_lv, ku_job['cislo_ku'])

            self.batch_content.append({
                'job_id': ku_job['id'],
                'ku_code': ku_job['cislo_ku'],
                'starting_lv': last_processed_lv
            })

        # add new ku jobs to batch
        for ku_job in waiting_ku_codes:
            self.batch_content.append({
                'job_id': ku_job['id'],
                'ku_code': ku_job['cislo_ku'],
                'starting_lv': 1
            })


def get_scraping_id():
    """Loads ID of scraping process."""

    scraping = db_handler.load(
        """SELECT id
            FROM log_scraping
            ORDER BY datum_zacatku DESC
            LIMIT 1""",
        values=None,
        single_item=True
    )
    return scraping['id'] if scraping else None


def init_new_scraping():
    """Inits new scraping of cadaster. Prepare all ku codes and set
    waiting status to them."""

    scraping_id = create_scraping()
    ku_list = load_all_ku()
    ku_jobs = []

    for ku in ku_list:
        ku_jobs.append({
            'id_scrapingu': scraping_id,
            'cislo_ku': ku['cislo_ku'],
            'stav': 'W'
        })

    db = db_handler.get_dataset()
    db['log_uloha'].insert_many(ku_jobs)

    return scraping_id


def create_scraping(name=None):
    """Creates new scraping process."""

    db = db_handler.get_dataset()
    scraping_id = db['log_scraping'].insert(dict(nazev=name))
    return scraping_id


def get_ku_jobs(scraping_id, status, limit=None):
    """Loads list of KU jobs by status.
    (R - Runinng, F - Finished, E - Error, W - Waiting)."""

    # db = db_handler.get_dataset()
    # ku_list = db['log_uloha'].find(
    #     id_scrapingu=scraping_id, stav=status, _limit=limit)

    query = """SELECT *
                FROM log_uloha
                WHERE id_scrapingu = {}
                  AND stav = '{}'""".format(scraping_id, status)
    if limit:
        query += ' LIMIT {}'.format(limit)

    ku_list = db_handler.load(query)
    return ku_list if ku_list else []


def get_last_processed_lv(job_id):
    """Loads last processed LV for job ID (job ID is connected) to KU,
    and with this connection we can distinguish data from different
    scrapings."""

    log_lv = db_handler.load(
        """SELECT MAX(cislo_lv) AS max_lv
            FROM log_lv
            WHERE id_ulohy = {}""".format(job_id)
    )
    return log_lv['max_lv'] if log_lv else 0


def load_all_ku():
    """Loads all KU."""

    # TODO add date range condition
    ku_list = db_handler.load("SELECT cislo_ku FROM ku")
    return ku_list if ku_list else []


def delete_whole_lv_item(lv, ku):
    """Delete all objects of LV in specified KU."""

    lv = db_handler.load(
        """SELECT id FROM lv
            WHERE cislo_lv = {} AND
                  cislo_ku = {}""".format(lv, ku)
    )
    id_lv = lv['id'] if lv else None

    if not id_lv:
        return

    # delete main table
    db_handler.delete("DELETE FROM lv WHERE id = {}".format(id_lv))

    # delete object tables
    where = "id_lv = {}".format(id_lv)
    for t in ['pozemek', 'stavebni_objekt', 'stavba', 'jednotka', 'vlastnici']:
        db_handler.delete("DELETE FROM {} WHERE {}".format(t, where))


def update_job_log(job_id, job_hash, status):
    """Updates job's hash and status."""

    db_handler.insert_or_update(
        """UPDATE log_uloha
            SET hash_ulohy = {}, stav = {}
            WHERE id = {}""".format(job_hash, status, job_id)
    )


class OperationCrawler():

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
        scrapyd = get_scrapyd()
        batch = create_scraping_batch()

        for process in batch:
            job_id = save_operation_log(process, status='R')

            scrapyd.schedule(
                OperationCrawler.project_name,
                OperationCrawler.spider_name,
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
        for type in OperationCrawler.operation_types:
            for wp in OperationCrawler.workplaces:
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
    OperationCrawler.run()
    # LandRegisterCrawler.run()
