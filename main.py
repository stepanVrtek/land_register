from land_register.spiders.title_deed_spider import TitleDeedSpider
from land_register import csv_reader
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import time

start_time = time.time()

OVERALL_MAX = 200
LV_MAX = 50
BATCH_COUNT = 200

process_count = 0
overall_count = 0

def get_process():
    process_count = 0
    return CrawlerProcess(get_project_settings())

def print_stats():
    e = int(time.time() - start_time)
    elapsed_time = print('{:02d}:{:02d}:{:02d}'.format(e // 3600, (e % 3600 // 60), e % 60))
    print("Processed {} requests in {}".format(overall_count, elapsed_time))

if __name__ == '__main__':

    process = get_process()

    ku_codes = csv_reader.get_ku_codes(
        'land_register/UI_KATASTRALNI_UZEMI.csv')

    for ku_code in ku_codes:
        for i in range(1, LV_MAX, 1):
            process_count += 1
            overall_count += 1
            process.crawl(TitleDeedSpider, ku_code=ku_code, lv_code=str(i))

            if process_count % BATCH_COUNT == 0:
                process.start(stop_after_crawl=False)
                process_count = 0
                print_stats()
                # process = get_process()

        if overall_count >= OVERALL_MAX:
            break