import scrapy
from urllib.parse import urljoin
from pprint import pprint
from scrapy.crawler import CrawlerProcess

BASE_URL = 'http://nahlizenidokn.cuzk.cz/'
KU_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$txtKU'
KU_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$btnKU'
LV_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$txtLV'
LV_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$btnVyhledat'
SEARCH_TXT = 'Vyhledat'

class LandRegisterListSpider(scrapy.Spider):
    name = "Land Register List Spider"

    def __init__(self, start_url, *args, **kwargs):
        self.start_urls.append(start_url)
        super.__init__(*args, **kwargs)

    start_urls = ['http://nahlizenidokn.cuzk.cz/VyberLV.aspx']

    # custom_settings = {
    #     'AUTOTHROTTLE_ENABLED': True
    # }


    def parse(self, response):
        yield scrapy.FormRequest.from_response(
            response,
            formdata = {
                KU_INPUT_ELEMENT: '600016',
                KU_SEARCH_BUTTON: SEARCH_TXT
            },
            callback=self.parse_second
        )


    def parse_second(self, response):
        if self.is_error_message(response):
            return

        ku_xpath = '//span[@id="ctl00_bodyPlaceHolder_vyberObecKU_vyberKU_lblKU"]/text()'
        print(response.xpath(ku_xpath).extract_first())

        yield scrapy.FormRequest.from_response(
            response,
            formdata = {
                LV_INPUT_ELEMENT: '1',
                LV_SEARCH_BUTTON: SEARCH_TXT
            },
            callback = self.parse_content
        )


    def parse_content(self, response):
        if self.is_error_message(response):
            return

        # owners
        # example: KU 733857, LV: 275
        owners_table = response.xpath('//table[@summary="Vlastníci, jiní oprávnění"]/tbody/tr')

        owners = []
        for row in owners_table:
            # header check
            if row.xpath('th/text()').extract_first() is not None:
                continue

            owners.append({
                'vlastnik': row.xpath('td[1]/text()').extract_first(),
                'podil': row.xpath('td[2]/text()').extract_first()
            })

        print(owners)

        # grounds
        grounds_table = response.xpath('//table[@summary="Pozemky"]/tbody/tr')
        for row in grounds_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)
            yield scrapy.Request(url, callback=self.parse_ground)

        # buildings
        buildings_table = response.xpath('//table[@summary="Stavby"]/tbody/tr')
        for row in buildings_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)
            yield scrapy.Request(url, callback=self.parse_building)

        # units
        # example: KU 733857, LV 2000
        units_table = response.xpath('//table[@summary="Jednotky"]/tbody/tr')
        for row in units:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)
            yield scrapy.Request(url, callback=self.parse_unit)


    def parse_ground(self, response):
        if self.is_error_message(response):
            return

        print("ground detail opened")

        # TODO parsing data:
        # general data

        # building on the groud

        # 'zpusoby ochrany nemovitosti'
        # parse_zom(self, response) - same for all subpages

        # BPEJ list
        # parse_bpej(self, response) - same for all subpages

        # 'omezeni vlastnickeho prava'
        # parse_ovp(self, response) - same for all subpages

        # other notes
        # parse_other_notes(self, response) - same for all subpages

        # operations - processing in another parse method
        operation_refs = self.get_refs_from_detail_table(
            response, 'Řízení, v rámci kterých byl k nemovitosti zapsán cenový údaj')
        for ref in operation_refs:
            yield scrapy.Request(ref, callback=self.parse_operation)


    def parse_building(self, response):
        if self.is_error_message(response):
            return

        print("building detail opened")

        # TODO parsing data:
        # general data

        # 'zpusoby ochrany nemovitosti'
        # parse_zom(self, response) - same for all subpages

        # 'omezeni vlastnickeho prava'
        # parse_ovp(self, response) - same for all subpages

        # other notes
        # parse_other_notes(self, response) - same for all subpages

        # operations - processing in another parse method
        operation_refs = self.get_refs_from_detail_table(
            response, 'Řízení, v rámci kterých byl k nemovitosti zapsán cenový údaj')
        for ref in operation_refs:
            yield scrapy.Request(ref, callback=self.parse_operation)


    def parse_unit(self, response):
        if self.is_error_message(response):
            return

        print("unit detail opened")

        # TODO parsing data:
        # general data

        # 'zpusoby ochrany nemovitosti'
        # parse_zom(self, response) - same for all subpages

        # 'omezeni vlastnickeho prava'
        # parse_ovp(self, response) - same for all subpages

        # other notes
        # parse_other_notes(self, response) - same for all subpages

        # operations - processing in another parse method
        operation_refs = self.get_refs_from_detail_table(
            response, 'Řízení, v rámci kterých byl k nemovitosti zapsán cenový údaj')
        for ref in operation_refs:
            yield scrapy.Request(ref, callback=self.parse_operation)


    def parse_operation(self, response):
        if self.is_error_message(response):
            return

        print("operation opened")


    def get_refs_from_detail_table(self, response, table_name):
        table = response.xpath('//table[@summary="{}"]/tbody/tr'.format(table_name))
        for row in table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)
            yield url

    def is_error_message(self, response):
        error_message = response.xpath(
            '//div[@id="ctl00_hlaseniOnMasterPage"]').extract_first() # ctl00_updatePanelHlaseniOnMasterPage
        print(error_message)

        # TODO distinguish between 'not found' and 'session expired' message

        return error_message is not None



process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(LandRegisterListSpider)
process.start()
