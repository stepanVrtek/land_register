from land_register import db_handler
from land_register.crawlers import utils
from land_register.crawlers.land_register_crawler import (
    get_scraping_id, get_ku_jobs
)
from datetime import datetime


def update_finished_statuses():
    running_py_jobs = get_ku_jobs(get_scraping_id(), 'R')

    scrapy_jobs = utils.get_scrapyd().list_jobs('land_register')
    running_scrapy_jobs = [j['id'] for j in scrapy_jobs['running']]

    for p in running_py_jobs:
        if p['hash_ulohy'] in running_scrapy_jobs:
            continue
        update_job_status(p['id'], 'F')


def update_job_status(job_id, status):
    db = db_handler.get_dataset()
    fields = dict(id=job_id, stav=status, datum_konce=datetime.now())
    db['log_uloha'].update(fields, ['id'])


if __name__ == '__main__':
    update_finished_statuses()
