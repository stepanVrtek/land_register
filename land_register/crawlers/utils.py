from scrapyd_api import ScrapydAPI


def get_scrapyd():
    return ScrapydAPI('http://localhost:6800')


def cancel_all_jobs():
    scrapyd = get_scrapyd()
    projects = scrapyd.list_projects()

    for project in projects['projects']:
        cancel_jobs(project)


def cancel_jobs(project, spider=None):
    scrapyd = get_scrapyd()
    jobs = scrapyd.list_jobs(project)

    active_jobs = jobs['running'] + jobs['pending']

    if spider:
        active_jobs = [j['id'] for j in active_jobs
            if j['spider'] == spider]
    else:
        active_jobs = [j['id'] for j in active_jobs]

    for job_id in active_jobs:
        cancel_job(project, job_id)


def cancel_job(project, job_id):
    scrapyd = get_scrapyd()
    scrapyd.cancel(project, job_id)
