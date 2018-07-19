import scrapy
from scrapy.crawler import CrawlerProcess
from urllib.parse import urljoin
from Spiders.items import NextURL
import json


class SeznamNemovitosti(scrapy.Spider):
    name = "SeznamNemovitosti"
    start_urls = ["http://nahlizenidokn.cuzk.cz/ZobrazObjekt.aspx?encrypted=6wCf7KhxP-YP6NrJmpLefYQXT9tsI0-CKVvDaw0RHZC1f0IuEeNFMLr4619wYYNBs9tgHz5DeVtqvIMkJbqVUryvGUq7yZUe4Eud9NSqL96bhVIjoDrSkw=="]

    def parse(self, response):
        main_url = "http://nahlizenidokn.cuzk.cz/"
        next_url = NextURL()

        for link in response.css(
                "[summary=Pozemky]"):  # dalsi moznost k "table.zarovnat" je ziskat primo prvni tabulku a to: "[summary=Pozemky]"
            next_url['url'] = link.css("a::attr(href)").extract()
            # kazdy link ma tvar: ZobrazObjekt.aspx?encrypted=*shitloadkodu==* ktery se musi spojit s predponou "http://nahlizenidokn.cuzk.cz/"
            yield next_url

        #filename = "seznam nemovitosti.txt"
        #with open(filename, 'wb') as outfile:
        #   json.dump(response.body, outfile)

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
