from scrapyd_api import ScrapydAPI


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
