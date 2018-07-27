import scrapy
from scrapy.crawler import CrawlerProcess
from multiprocessing import Process
from twisted.internet import reactor
from land_register.spiders.title_deed_spider import TitleDeedSpider
from land_register import csv_reader
from land_register import generate_proxies
from scrapy.utils.project import get_project_settings
import time

start_time = time.time()

OVERALL_MAX = 300
BATCH_MAX = 20
LV_MAX = 3
overall_count = 0
batch_count = 0


def print_stats():
    e = int(time.time() - start_time)
    elapsed_time = print('{:02d}:{:02d}:{:02d}'.format(
        e // 3600, (e % 3600 // 60), e % 60))
    print('time: {}'.format(e))
    print("Processed {} requests in {}".format(overall_count, elapsed_time))


def run_spider(batch):
    generate_proxies.generate()
    process = CrawlerProcess(get_project_settings())
    for b in batch:
        process.crawl(TitleDeedSpider,
                      ku_code=b['ku_code'], lv_code=b['lv_code'])

    process.start()
    print_stats()


if __name__ == '__main__':
    ku_codes = csv_reader.get_ku_codes(
        'land_register/UI_KATASTRALNI_UZEMI.csv')

    batch = []

    for ku_code in ku_codes:
        for i in range(1, LV_MAX, 1):
            overall_count += 1
            batch_count += 1
            batch.append({
                'ku_code': ku_code,
                'lv_code': str(i)
            })

            if batch_count == BATCH_MAX:
                p1 = Process(target=run_spider, args=(batch.copy(),))
                p1.start()
                p1.join()
                batch_count = 0
                batch.clear()

        if overall_count >= OVERALL_MAX:
            break