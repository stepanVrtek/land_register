from land_register.spiders.title_deed_spider import TitleDeedSpider
from land_register import csv_reader
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from land_register import generate_proxies


if __name__ == '__main__':

    generate_proxies.generate()
    process = CrawlerProcess(get_project_settings())

    for i in range(1,5):
        process.crawl(TitleDeedSpider, ku_code='600041', lv_code=str(3500 + i))
        process.crawl(TitleDeedSpider, ku_code='600083', lv_code=str(20 + i))
        process.crawl(TitleDeedSpider, ku_code='663409', lv_code=str(1 + i))
        process.crawl(TitleDeedSpider, ku_code='771473', lv_code=str(280 + i))
        process.crawl(TitleDeedSpider, ku_code='917923', lv_code=str(1 + i))

    process.start()