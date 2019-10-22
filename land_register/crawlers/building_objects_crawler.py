from land_register.crawlers import utils
from land_register import db_handler
from scrapy.utils.project import get_project_settings


class LandRegisterCrawler:
    """Class for scheduling of scraping all data in land register."""

    project_name = 'land_register'
    spider_name = 'BuildingObjectsSpider'

    @staticmethod
    def run():
        batch_data = prepare_batch()

        scrapyd = utils.get_scrapyd()
        for batch_ids in batch_data:
            job_hash = scrapyd.schedule(
                LandRegisterCrawler.project_name,
                LandRegisterCrawler.spider_name,
                ids=batch_ids
            )
            # set ids' statuses to processing
            for _id in batch_ids:
                update_item_status(_id, status='P')


def prepare_batch():
    settings = get_project_settings()
    max_items = settings['MAX_STAVEBNI_OBJEKTY_IN_BATCH']
    max_processes = settings['MAX_STAVEBNI_OBJEKTY_PROCESSES']

    # check current running scrapyd processes
    active_processes = utils.sum_active_jobs(
        LandRegisterCrawler.project_name, LandRegisterCrawler.spider_name
    )

    max_processes -= active_processes
    if max_processes < 1:
        return []

    items_to_process = max_items * max_processes

    db = db_handler.get_dataset()
    results = db['stavebni_objekt_ref'].find(
        stav_scrapingu='W', _limit=items_to_process, order_by='id'
    )

    batch = []
    single_list = []
    for r in results:
        if len(single_list) > max_processes:
            batch.append(single_list.copy())
            single_list = []

        single_list.append(r['id'])

    if single_list:
        batch.append(single_list)

    return batch


def update_item_status(id, status):
    db = db_handler.get_dataset()
    db['stavebni_objekt_ref'].update(dict(id=id, stav_scrapingu=status), ['id'])


if __name__ == '__main__':
    LandRegisterCrawler.run()
