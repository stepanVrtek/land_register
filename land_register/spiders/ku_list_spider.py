import scrapy
from pprint import pprint
from urllib.parse import urljoin

BASE_URL = 'https://www.cuzk.cz/'
START_URL = 'https://www.cuzk.cz/Katastr-nemovitosti/Digitalizace-a-vedeni-katastralnich-map/Digitalizace-katastralnich-map/Seznam-katastralnich-uzemi-bez-parcel-v-ZE.aspx'


class KuListSpider(scrapy.Spider):
    name = "KuListSpider"
    start_urls = [START_URL]

    def parse(self, response):
        """Parse list of KP (workplaces) and process every of them."""

        wp_table = response.xpath('(//table)[1]/tbody/tr')
        for row in wp_table:
            ref = row.xpath('td[1]/a/@href').extract_first()
            if ref is None:
                continue

            url = urljoin(BASE_URL, ref)
            yield scrapy.Request(url, callback=self.parse_ku)

    def parse_ku(self, response):
        """Parse and save all data for KU."""

        ku_list = []
        ku_table = response.xpath(
            '//table[@class="tabulka"]/tbody/tr[@class="radek"]')
        for index, row in enumerate(ku_table):
            ku_item = {
                'cislo_ku': row.xpath('td[2]/text()').extract_first(),
                # 'cislo_pracoviste': row.xpath('td[3]/text()').extract_first(),
                'nazev_ku': row.xpath('td[4]/a/text()').extract_first()
            }
            ku_list.append(ku_item)

        pprint(ku_list)
