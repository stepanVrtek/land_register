import scrapy
from urllib.parse import urljoin
from pprint import pprint
from scrapy.crawler import CrawlerProcess
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import random

BASE_URL = 'https://nahlizenidokn.cuzk.cz/'
START_URL = 'https://nahlizenidokn.cuzk.cz/VyberLV.aspx'
KU_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$txtKU'
KU_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$btnKU'
LV_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$txtLV'
LV_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$btnVyhledat'
SEARCH_TXT = 'Vyhledat'

class TestItem(scrapy.Item):
    ku_code = scrapy.Field()
    lv_code = scrapy.Field()
    request_type = scrapy.Field()
    download_latency = scrapy.Field()
    response_status = scrapy.Field()
    valid_request = scrapy.Field()

class TitleDeedSpider(scrapy.Spider):
    name = "TitleDeedSpider"

    def __init__(self, ku_code = '', lv_code = '', **kwargs):
        self.ku_code = ku_code
        self.lv_code = lv_code

        # self.change_proxy()
        self.overall_counter = 0
        self.proxy_counter = 0

        super().__init__(**kwargs)

    start_urls = [START_URL]

    def response_is_ban(self, request, response):
        return response.status == 403

    def get_test_item(self, response):
        item = TestItem()
        item['ku_code'] = self.ku_code
        item['lv_code'] = self.lv_code
        item['download_latency'] = response.meta['download_latency']
        item['response_status'] = response.status
        item['valid_request'] = True
        return item

    # def change_proxy(self):
    #     ua = UserAgent() # From here we generate a random user agent
    #     proxies_req = Request('https://www.sslproxies.org/')
    #     proxies_req.add_header('User-Agent', ua.random)
    #     proxies_doc = urlopen(proxies_req).read().decode('utf8')

    #     soup = BeautifulSoup(proxies_doc, 'html.parser')
    #     proxies_table = soup.find(id='proxylisttable')

    #     proxies = [] # Will contain proxies [ip, port]
    #     for row in proxies_table.tbody.find_all('tr'):
    #         proxies.append({
    #           'ip':   row.find_all('td')[0].string,
    #           'port': row.find_all('td')[1].string
    #         })

    #     random_choice = random.choice(proxies)
    #     self.proxy = 'https://'+ random_choice['ip'] + ':' + random_choice['port']

    # def increment_request_counters(self):
    #     self.overall_counter += 1
    #     self.proxy_counter += 1

    #     if self.proxy_counter > 180:
    #         self.change_proxy()
    #         self.proxy_counter = 1

    def parse(self, response):
        """Parse KU code (kod katastralneho uzemia)"""

        test_item = self.get_test_item(response)
        test_item['request_type'] = 'first page loading'
        yield test_item

        yield scrapy.FormRequest.from_response(
            response,
            # meta = {'proxy': self.proxy},
            formdata = {
                KU_INPUT_ELEMENT: self.ku_code,
                KU_SEARCH_BUTTON: SEARCH_TXT
            },
            callback=self.parse_second
        )

    def parse_second(self, response):
        """Parse LV code (kod listu vlastnictva)"""
        test_item = self.get_test_item(response)
        test_item['request_type'] = 'KU input'

        if self.is_error_message(response):
            test_item['valid_request'] = False

        yield test_item

        if test_item['valid_request'] == False:
            return

        # ku_xpath = '//span[@id="ctl00_bodyPlaceHolder_vyberObecKU_vyberKU_lblKU"]/text()'
        # print(response.xpath(ku_xpath).extract_first())

        yield scrapy.FormRequest.from_response(
            response,
            # meta = {'proxy': self.proxy},
            formdata = {
                LV_INPUT_ELEMENT: self.lv_code,
                LV_SEARCH_BUTTON: SEARCH_TXT
            },
            callback = self.parse_content
        )

    def parse_content(self, response):
        """Parce content of LV"""
        test_item = self.get_test_item(response)
        test_item['request_type'] = 'LV input'

        if self.is_error_message(response):
            test_item['valid_request'] = False

        yield test_item

        if test_item['valid_request'] == False:
            return

        # owners - data without ref, uncomment to parse this
        # example: KU 733857, LV: 275
        # owners_table = response.xpath('//table[@summary="Vlastníci, jiní oprávnění"]/tbody/tr')
        #
        # owners = []
        # for row in owners_table:
        #     # header check
        #     if row.xpath('th/text()').extract_first() is not None:
        #         continue
        #
        #     owners.append({
        #         'vlastnik': row.xpath('td[1]/text()').extract_first(),
        #         'podil': row.xpath('td[2]/text()').extract_first()
        #     })
        #
        # print(owners)

        # grounds
        grounds_table = response.xpath('//table[@summary="Pozemky"]/tbody/tr')
        for row in grounds_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)
            yield scrapy.Request(url, callback=self.parse_ground) # meta = {'proxy': self.proxy}

        # buildings
        buildings_table = response.xpath('//table[@summary="Stavby"]/tbody/tr')
        for row in buildings_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)
            yield scrapy.Request(url, callback=self.parse_building)

        # units
        # example: KU 733857, LV 2000
        units_table = response.xpath('//table[@summary="Jednotky"]/tbody/tr')
        for row in units_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)
            yield scrapy.Request(url, callback=self.parse_unit)

    def parse_ground(self, response):
        if self.is_error_message(response):
            return

        test_item = self.get_test_item(response)
        test_item['request_type'] = 'ground detail opened'
        yield test_item

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

        test_item = self.get_test_item(response)
        test_item['request_type'] = 'building detail opened'
        yield test_item

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

        test_item = self.get_test_item(response)
        test_item['request_type'] = 'unit detail opened'
        yield test_item

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

        test_item = self.get_test_item(response)
        test_item['request_type'] = 'operation detail opened'
        yield test_item

        print("operation detail opened")


    def get_refs_from_detail_table(self, response, table_name):
        table = response.xpath('//table[@summary="{}"]/tbody/tr'.format(table_name))
        for row in table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)
            yield url

    def is_error_message(self, response):
        error_message = response.xpath(
            '//div[@id="ctl00_hlaseniOnMasterPage"]').extract_first() # ctl00_updatePanelHlaseniOnMasterPage

        # TODO distinguish between 'not found' and 'session expired' message

        return error_message is not None