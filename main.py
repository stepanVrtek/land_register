import Spiders.SeznamNemovitosti
import Spiders.lr_list_spider
import requests
import time
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(Spiders.lr_list_spider)
process.start()

