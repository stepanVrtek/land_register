from land_register.spiders.title_deed_spider import TitleDeedSpider
from land_register import csv_reader
from scrapy.crawler import CrawlerProcess

if __name__ == '__main__':
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    count = 0

    ku_codes = csv_reader.get_ku_codes('land_register/UI_KATASTRALNI_UZEMI.csv')
    for ku_code in ku_codes:
        count += 1
        for i in range(1, 50):
            process.crawl(TitleDeedSpider, ku_code = ku_code, lv_code = str(i))
        if count == 10:
            break

    process.start()
