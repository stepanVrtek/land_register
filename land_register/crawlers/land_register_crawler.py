from datetime import datetime
from datetime import date

from land_register.crawlers import utils
from land_register import db_handler
from scrapy.utils.project import get_project_settings


class LandRegisterCrawler():
    """Class for scheduling of scraping all data in land register."""

    project_name = 'land_register'
    spider_name = 'LandRegisterSpider'

    @staticmethod
    def run():
        scraping_batch = ScrapingBatch()
        scraping_batch.create()

        scrapyd = utils.get_scrapyd()
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

        self._create_new_scraping = False

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
        if self._create_new_scraping:
            close_scraping(self.scraping_id)
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
        ku_to_add -= len(error_ku_jobs)

        # get list of new ku which are waiting to process
        # (only if there are still free slots)
        waiting_ku_codes = []
        if ku_to_add:
            waiting_ku_codes = get_ku_jobs(
                self.scraping_id, 'W', limit=ku_to_add)
            ku_to_add -= len(waiting_ku_codes)

        # if there are no ku jobs to add, create new scraping
        if not error_ku_jobs and not waiting_ku_codes:
            self._create_new_scraping = True
            return

        # add error and new jobs to batch
        for ku_job in error_ku_jobs:
            last_processed_lv = get_last_processed_lv(ku_job['id'])

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

    db = db_handler.get_dataset()
    result = db.query("""
        SELECT MAX(id) as id
          FROM log_scraping"""
    )
    for r in result:
        return r['id'] if r['id'] else None


def init_new_scraping():
    """Inits new scraping of cadaster. Prepare all ku codes and set
    waiting status to them."""

    scraping_id = create_scraping()
    ku_list = load_all_ku()
    ku_jobs = []

    for ku in ku_list:
        ku_jobs.append({
            'id_scrapingu': scraping_id,
            'cislo_ku': ku,
            'stav': 'W'
        })

    db = db_handler.get_dataset()
    db['log_uloha'].insert_many(ku_jobs)

    return scraping_id


def create_scraping(name=None):
    """Creates new scraping process."""

    db = db_handler.get_dataset()
    fields = dict(nazev=name, datum_zacatku=datetime.now())
    scraping_id = db['log_scraping'].insert(fields)
    return scraping_id


def close_scraping(scraping_id):
    """Set current date as date of end for scraping."""

    db = db_handler.get_dataset()
    fields = dict(id=scraping_id, datum_konce=datetime.now())
    db['log_scraping'].update(fields, ['id'])


def get_ku_jobs(scraping_id, status, limit=None):
    """Loads list of KU jobs by status.
    (R - Runinng, F - Finished, E - Error, W - Waiting)."""

    db = db_handler.get_dataset()
    results = db['log_uloha'].find(
        id_scrapingu=scraping_id, stav=status, _limit=limit
    )
    return [r for r in results]


def get_last_processed_lv(job_id):
    """Loads last processed LV for job ID (job ID is connected) to KU,
    and with this connection we can distinguish data from different
    scrapings."""

    db = db_handler.get_dataset()
    result = db.query("""
        SELECT MAX(cislo_lv) as max_lv
          FROM log_lv
          WHERE id_ulohy = {}""".format(job_id)
    )
    for r in result:
        return r['max_lv'] if r['max_lv'] else 0


def load_all_ku():
    """Loads all KU."""

    db = db_handler.get_dataset()
    results = db['ku'].all()

    today = date.today()
    ku_list = []
    for r in results:
        if r['plati_od']:
            if r['plati_od'] > today:
                continue
        if r['plati_do']:
            if r['plati_do'] < today:
                continue
        ku_list.append(r['cislo_ku'])

    return ku_list


def delete_whole_lv_item(lv, ku):
    """Delete all objects of LV in specified KU."""

    db = db_handler.get_dataset()
    result = db['lv'].find_one(cislo_lv=lv, cislo_ku=ku)

    id_lv = result['id'] if result else None

    if not id_lv:
        return

    # delete main table
    db['lv'].delete(id=id_lv)

    # delete object tables
    for t in ['pozemek', 'stavebni_objekt', 'stavba', 'jednotka', 'vlastnici']:
        db[t].delete(id_lv = id_lv)


def update_job_log(job_id, job_hash, status):
    """Updates job's hash and status."""

    db = db_handler.get_dataset()
    fields = dict(id=job_id, hash_ulohy=job_hash, stav=status)

    # if status is R (running), set date of create
    if status == 'R':
        fields['datum_zacatku'] = datetime.now()

    db['log_uloha'].update(fields, ['id'])



if __name__ == '__main__':
    LandRegisterCrawler.run()
