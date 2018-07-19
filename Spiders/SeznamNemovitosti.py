import scrapy
from scrapy.crawler import CrawlerProcess
from urllib.parse import urljoin
from Spiders.items import NextURL
import json
import pickle


class SeznamNemovitosti(scrapy.Spider):
    name = "SeznamNemovitosti"
    start_urls = [
        "http://nahlizenidokn.cuzk.cz/ZobrazObjekt.aspx?encrypted=e7hd04PXsVGmAz0h5NDhBAfCTlzwXmc0i23fn3r6-g7IywYksooFB3_01qXCPjnuFAPjZlNdi-oE_-giU2wylKvTmiA3LcN08EFD46V4eCNKsc9R4gUacw=="]

    def parse(self, response):
        main_url = "http://nahlizenidokn.cuzk.cz/"
        next_url = NextURL()

        for link in response.css("[summary=Pozemky]"):  # "table.zarovnat" = vsechny tabulky, prvni tabulka = "[summary=Pozemky]"
            next_url['url'] = link.css("a::attr(href)").extract() #link: ZobrazObjekt.aspx?encrypted=*shitloadkodu==* ktery se musi spojit s predponou "http://nahlizenidokn.cuzk.cz/"
            with open('urls.txt', 'w') as file:
                file.write(json.dumps(next_url.__dict__)) #delete me, I am here just for debug
            yield next_url

        next_page = response.css('table.zarovnat a::attr(href)').extract_first()
        if next_page is not None:
            next_page = urljoin(main_url, next_page)
            yield response.follow(next_page,
                                  callback=self.parse)  # volam ten stejny parser jako na seznam nemovitosti, coz neni dobre, musim udelat dalsi parser


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(SeznamNemovitosti)
process.start()
