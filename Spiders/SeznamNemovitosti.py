import json
from urllib.parse import urljoin
from Spiders.items import UrlItem
from Spiders.items import Nemovitosti
import scrapy
from scrapy.crawler import CrawlerProcess


class SeznamNemovitosti(scrapy.Spider):
    name = "SeznamNemovitosti"
    start_urls = [
        "http://nahlizenidokn.cuzk.cz/ZobrazObjekt.aspx?encrypted=bUrsf-lRpqts8K2Hs6RMGr7HX0ngePvoeENqwacpHvYeDAaFHPMjHGC-ywuB0VXlshMm06eThAMsf0fpS2dnMs-sJgVzO4lUJDDAy5lEnHupuz_LVpSKaw=="]

    def parse(self, response):
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
        nemovitosti['lv'] = response.xpath('//*[@id="content"]/table[1]/tbody/tr[1]/td[2]/strong/text()').extract()
        #nemovitosti['ku'] = response.css('a::attr(href) a::text').extract_first()
        print(nemovitosti)

        #tímto loopem projedu každou další stránku a spustím nad ní další parser: scrap_this_page
        list_of_urls = self.parse_me_url(next_url)
        for x in list_of_urls:
            self.scrap_this_page(x)

    def parse_me_url(self, link_dict=UrlItem()):
        main_url = "http://nahlizenidokn.cuzk.cz/"
        complete_url = []
        if len(link_dict) > 0:
            urls = link_dict["url"] #z dictionary si vytáhnu value pro key "url", která je stringem s url
            for x in urls:
                complete_url.append(urljoin(main_url, x))  # spojuji hlavni cast odkazu s vyparsovanou casti odkazu
        return complete_url

    def scrap_this_page(self, url):
        r = scrapy.http.Request(url)
        return r


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(SeznamNemovitosti)
process.start()
