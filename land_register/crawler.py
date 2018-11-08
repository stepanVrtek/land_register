import spiders
import csv_reader
import db_handler

from scrapyd_api import ScrapydAPI
from datetime import datetime
from datetime import timedelta

from scrapy.utils.project import get_project_settings


def get_scrapyd():
    return ScrapydAPI('http://localhost:6800')


class LandRegisterCrawler():
    project_name = 'land_register'
    spider_name = 'TitleDeedSpider'

    @staticmethod
    def run():
        scraping_batch = ScrapingBatch()
        scraping_batch.create()
        if not scraping_batch.batch_content:
            print('Vsetky procesy su vytazene. Dalsie KU nebudu pridane.')
            return
        else:
            print('Bude pridanych {} novych KU pre spracovanie.'.format(
                len(scraping_batch.batch_content)))

        scraping_batch.save_batch_log()

        scrapyd = get_scrapyd()
        for ku_code in scraping_batch.batch_content:
            job_id = scrapyd.schedule(
                LandRegisterCrawler.project_name,
                LandRegisterCrawler.spider_name,
                ku_code = ku_code
            )
            scraping_batch.save_job_log(job_id, ku_code)


class ScrapingBatch():

    def __init__(self):
        self.scraping_num = None
        self.batch_num = None
        self.batch_content = None
        self.batch_id = None

    def create(self):
        self.last_scraping_log = get_last_scraping_log()
        self.scraping_num = self.last_scraping_log.get('cislo_scrapingu', 0)
        self.batch_num = self.last_scraping_log.get('cislo_davky', 0) + 1
        self.prepare_batch_content()

    def prepare_batch_content(self):
        last_batch = eval(self.last_scraping_log.get('davka', '[]'))

        ku_in_batch = ScrapingBatch.get_num_of_items_in_next_batch()
        ku_list = csv_reader.get_ku_codes('UI_KATASTRALNI_UZEMI.csv')

        if last_batch:
            last_ku = last_batch[-1]
            idx = ku_list.index(last_ku) + 1

            # if we are at the end of the ku list,
            # we have to continue from beginning
            if idx < len(ku_list):
                self.batch_content = ku_list[idx:idx+ku_in_batch]
                return

        # in this case set new scraping_number
        self.scraping_num += 1
        self.batch_content = ku_list[:ku_in_batch]

    @staticmethod
    def get_num_of_items_in_next_batch():
        """Default number of subtracted by number of running jobs."""

        scrapyd = get_scrapyd()
        jobs = scrapyd.list_jobs(LandRegisterCrawler.project_name)

        pending = [j for j in jobs['pending']
            if j['spider'] == LandRegisterCrawler.spider_name]
        running = [j for j in jobs['running']
            if j['spider'] == LandRegisterCrawler.spider_name]

        settings = get_project_settings()
        ku_in_batch = settings['MAX_PROCESSES_IN_BATCH']
        print('ku in batch: {}'.format(ku_in_batch))
        ku_in_batch -= len(pending)
        ku_in_batch -= len(running)
        return ku_in_batch

    def save_batch_log(self):
        query = """INSERT INTO log_davky(
                    cislo_scrapingu, cislo_davky, davka)
                    VALUES (%s, %s, %s)"""
        values = (
            self.scraping_num,
            self.batch_num,
            str(self.batch_content)
        )
        self.batch_id = db_handler.insert_or_update(query, values)

    def save_job_log(self, job_id, ku):
        query = """INSERT INTO log_ulohy(
                    id_ulohy, id_davky, cislo_ku)
                    VALUES (%s, %s, %s)"""
        values = (
            job_id,
            self.batch_id,
            ku
        )
        db_handler.insert_or_update(query, values)


def get_last_scraping_log():
    log = db_handler.load(
        """SELECT *
            FROM log_davky
            ORDER BY datum DESC
            LIMIT 1""",
        values=None,
        single_item=True
    )
    return log if log else {}


def get_uncompleted_ku(scraping_number=None):
    valid_lvs = load_highest_valid_lvs() # use scraping number
    invalid_lvs = load_highest_invalid_lvs() # use scraping number

    settings = get_project_settings()
    max_invalid_items_in_row = settings['MAX_INVALID_ITEMS_IN_ROW']

    uncompleted_ku = []
    for ku, _ in valid_lvs.items():
        highest_valid_lv = valid_lvs[ku]
        highest_invalid_lv = invalid_lvs[ku]

        invalid_lvs_in_row = highest_invalid_lv - highest_valid_lv
        if invalid_lvs_in_row < 0:
            invalid_lvs_in_row = 0

        if invalid_lvs_in_row != max_invalid_items_in_row:
            highest_checked_lv = (highest_invalid_lv
                if highest_invalid_lv > highest_valid_lv
                else highest_valid_lv)

            uncompleted_ku.append(ku)

        print('KU: {}, neuspesne pokusy hladania platnych LV: {}'.format(
            ku, invalid_lvs_in_row))

    # get only non running and non pending jobs
    scrapyd = get_scrapyd()
    jobs = scrapyd.list_jobs(LandRegisterCrawler.project_name)

    active_jobs = [j['id'] for j in jobs['pending']
        if j['spider'] == LandRegisterCrawler.spider_name]
    active_jobs += [j['id'] for j in jobs['running']
        if j['spider'] == LandRegisterCrawler.spider_name]

    # exclude active ku (if exist)
    if active_jobs:
        active_ku = load_ku_codes_by_job_ids(active_jobs)
        uncompleted_ku = [u for u in uncompleted_ku if u not in active_ku]

    return uncompleted_ku

def load_highest_valid_lvs(**kwargs):
    valid_lvs = db_handler.load(
        """SELECT cislo_ku, MAX(cislo_lv)
            FROM log_lv
            WHERE existuje = true
            GROUP BY cislo_ku""",
        with_col_names=False
    )
    return dict(valid_lvs) if valid_lvs else None

def load_highest_invalid_lvs(**kwargs):
    invalid_lvs = db_handler.load(
        """SELECT cislo_ku, MAX(cislo_lv)
            FROM log_lv
            WHERE existuje = false
            GROUP BY cislo_ku""",
        with_col_names=False
    )
    return dict(invalid_lvs) if invalid_lvs else None

def load_ku_codes_by_job_ids(job_ids):
    values = ', '.join(map(str, job_ids))
    ku_codes = db_handler.load(
        """SELECT cislo_ku
            FROM log_ulohy
            WHERE id_ulohy IN {}""".format(values),
        with_col_names=False
    )

def delete_all_jobs():
    scrapyd = get_scrapyd()
    projects = scrapyd.list_projects()

    for project in projects['projects']:
        delete_jobs(project)

def delete_jobs(project):
    scrapyd = get_scrapyd()
    jobs = scrapyd.list_jobs(project)

    running_jobs = [j['id'] for j in jobs['pending']]




class OperationCrawler(LandRegisterCrawler):
    spider_name = 'OperationsSpider'
    _workplaces = [
        20, 101, 301, 302, 303, 305, 306, 307, 308, 701, 731, 702, 703, 704,
        706, 735, 738, 712, 713, 402, 403, 409, 602, 604, 605, 607, 610, 501,
        532, 504, 505, 608, 801, 802, 803, 831, 804, 806, 807, 832, 835, 811,
        805, 709, 808, 809, 603, 606, 609, 611, 401, 404, 435, 406, 405, 407,
        408, 410, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212,
        231, 502, 503, 506, 507, 508, 531, 509, 510, 533, 601, 707, 741, 304,
        710, 746, 714, 740, 708, 711, 742, 737, 836, 810, 705
    ]
    _operation_types = ['V', 'Z']

    @staticmethod
    def run():
        scrapyd = get_scrapyd()

        start_date = datetime(2018, 9, 24)
        end_date = datetime.now()
        date = end_date

        while date >= start_date:
            formatted_date = date.strftime('%d.%m.%Y')

            for type in OperationCrawler._operation_types:
                for wp in OperationCrawler._workplaces:
                    scrapyd.schedule(
                        OperationCrawler.project_name,
                        OperationCrawler.spider_name,
                        workplace = str(wp),
                        type = type,
                        date = formatted_date
                    )

            date -= timedelta(days = 1)

if __name__ == '__main__':
    # OperationCrawler.run()
    LandRegisterCrawler.run()
