import json
from urllib.parse import urljoin
from Spiders.items import UrlItem
from Spiders.items import Nemovitosti
import scrapy
from scrapy.crawler import CrawlerProcess


class SeznamNemovitosti(scrapy.Spider):
    name = "SeznamNemovitosti"
    start_urls = [
        "http://nahlizenidokn.cuzk.cz/ZobrazObjekt.aspx?encrypted=7WQQZVoVO_aSdiGFLLM8HPXMU8EODCOTXC2cd9vWw53pjByIQmcOpfFVVIaIndVRV8WhAopHX-zd7egkrtbflHIqnjnXy74ayfNyo9s5DefMSQNV7Mq-MA=="]

    def parse(self, response):
        main_url = "http://nahlizenidokn.cuzk.cz/"
        next_url = UrlItem()

        #získávám odkazy na parcely, které chceme scrapovat
        for link in response.css(
                "[summary=Pozemky]"):  # "table.zarovnat" = vsechny tabulky, prvni tabulka = "[summary=Pozemky]"
            next_url['url'] = link.css(
                "a::attr(href)").extract()  # link: ZobrazObjekt.aspx?encrypted=*shitloadkodu==* ktery se musi spojit s predponou "http://nahlizenidokn.cuzk.cz/"
            with open('urls.txt', 'w') as file:
                file.write(json.dumps(next_url.__dict__))  # delete me, I am here just for debug
            yield next_url

        nemovitosti = Nemovitosti()
        nemovitosti['lv'] = response.xpath('//table[1]/tbody/tr[1]/td[2]/strong[text()]').extract()
        #nemovitosti['ku'] = response.css('a::attr(href) a::text').extract_first()
        print("aha")
        print(nemovitosti)

        #z proměnné typu "dict" vytvořím list obsahující jednotlivé odkazy
        list_of_urls = self.parse_me_url(next_url)

        next_page = response.css('table.zarovnat a::attr(href)').extract_first()
        if next_url is not None:
            next_page = urljoin(main_url, next_page)
            yield response.follow(next_page,
                                  callback=self.parse)  # volam ten stejny parser jako na seznam nemovitosti, coz neni dobre, musim udelat dalsi parser

    def parse_me_url(self, link_dict=UrlItem()):
        if len(link_dict) > 0:
            urls = link_dict["url"] #z dictionary si vytáhnu value pro key "url", která je stringem s url
            urls_as_string = str(urls)
            list_of_urls = [urls_as_string.split(",")]
            return list_of_urls

    def scrap_this_page(self, url):
        r = 0
        return r


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(SeznamNemovitosti)
process.start()
