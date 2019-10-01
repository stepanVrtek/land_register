from scrapyd_api import ScrapydAPI
from land_register import db_handler
from land_register.crawlers.land_register_crawler import get_scraping_id


def get_scrapyd():
    return ScrapydAPI('http://localhost:6800')


def cancel_all_jobs(jobs_statuses=['running', 'pending', 'finished']):
    scrapyd = get_scrapyd()
    projects = scrapyd.list_projects()

    for project in projects:
        cancel_jobs(project, jobs_statuses)


def cancel_jobs(project, spider=None, jobs_statuses=['running', 'pending']):
    scrapyd = get_scrapyd()
    jobs = scrapyd.list_jobs(project)

    jobs_to_cancel = []
    for s in jobs_statuses:
        jobs_to_cancel += jobs[s]

    if spider:
        jobs_to_cancel = [j['id'] for j in jobs_to_cancel
            if j['spider'] == spider]
    else:
        jobs_to_cancel = [j['id'] for j in jobs_to_cancel]

    for job_id in jobs_to_cancel:
        cancel_job(project, job_id)


def cancel_job(project, job_id):
    scrapyd = get_scrapyd()
    scrapyd.cancel(project, job_id)


def sum_active_jobs(project, spider):
    scrapyd = get_scrapyd()
    jobs = scrapyd.list_jobs(project)

    active_jobs = []
    for s in ['running', 'pending']:
        active_jobs += [j for j in jobs[s] if j['spider'] == spider]

    return len(active_jobs)


def check_completness(ku_code):
    scraping_id = get_scraping_id()

    db = db_handler.get_dataset()
    max_valid_lvs = db.query("""
        SELECT MAX(log_lv.cislo_lv), log_lv.cislo_ku
          FROM log_lv
          INNER JOIN log_uloha
            ON log_lv.id_ulohy = log_uloha.id
          WHERE log_uloha.id_scrapingu = 53
          GROUP BY log_lv.cislo_ku, log_lv.existuje
          ORDER BY log_lv.cislo_ku ASC""".format(scraping_id)
    )

    """
        SELECT MAX(cislo_lv), cislo_ku, existuje
          FROM log_lv
          WHERE cislo_ku = '600016'
          GROUP BY cislo_ku, existuje
          ORDER BY cislo_ku ASC"""

    max_invalid_lvs = db.query("""
        SELECT MAX(cislo_lv) AS max_invalid_lv, cislo_ku
          FROM log_lv
          WHERE existuje = 0"""
    )
