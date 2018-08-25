import scrapy
from urllib.parse import urljoin
from pprint import pprint
from scrapy.crawler import CrawlerProcess
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import random
from unidecode import unidecode

BASE_URL = 'https://nahlizenidokn.cuzk.cz/'
START_URL = 'https://nahlizenidokn.cuzk.cz/VyberLV.aspx'
KU_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$txtKU'
KU_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$btnKU'
LV_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$txtLV'
LV_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$btnVyhledat'
SEARCH_TXT = 'Vyhledat'
MAX_LV_NOT_FOUND_IN_ROW = 100


class TitleDeedSpider(scrapy.Spider):
    name = "TitleDeedSpider"
    start_urls = [START_URL]

    def __init__(self, ku_code='', **kwargs):
        self.ku_code = ku_code
        self.invalid_in_row = 0
        self.total_count = 0
        super().__init__(**kwargs)

    def response_is_ban(self, request, response):
        return response.status == 403 or response.status == 500


    def parse(self, response):
        """Parse KU code (kod katastralneho uzemia)"""

        # print('-----------------PARSE method')
        print('cislo lv: {}, cislo ku: {}'.format(response.meta.get('cislo_lv', 1), self.ku_code))

        self.resp = response

        yield scrapy.FormRequest.from_response(
            response,
            meta={
                'cislo_lv': response.meta.get('cislo_lv', 1)
            },
            formdata={
                KU_INPUT_ELEMENT: self.ku_code,
                KU_SEARCH_BUTTON: SEARCH_TXT
            },
            callback=self.enter_lv_code
        )

    def parse_again(self, response):
        """Parse KU code (kod katastralneho uzemia)"""

        # print('-----------------PARSE method')
        print('cislo lv: {}, cislo ku: {}'.format(response.meta.get('cislo_lv', 1), self.ku_code))
        # print('velkost requestu: {}'.format(response.headers['Content-Length']))

        yield scrapy.FormRequest.from_response(
            self.resp, # response,
            meta={
                'cislo_lv': response.meta.get('cislo_lv')
            },
            formdata={
                KU_INPUT_ELEMENT: self.ku_code,
                KU_SEARCH_BUTTON: SEARCH_TXT
            },
            callback=self.enter_lv_code,
            dont_filter = True
        )

    def enter_lv_code(self, response):
        """Parse LV code (kod listu vlastnictva)"""

        if self.is_error_message(response):
            return

        self.ku_response = response

        # ku_xpath = '//span[@id="ctl00_bodyPlaceHolder_vyberObecKU_vyberKU_lblKU"]/text()'
        # print(response.xpath(ku_xpath).extract_first())

        yield scrapy.FormRequest.from_response(
            response,
            meta={'cislo_lv': response.meta.get('cislo_lv')},
            formdata={
                LV_INPUT_ELEMENT: str(response.meta['cislo_lv']),
                LV_SEARCH_BUTTON: SEARCH_TXT
            },
            callback=self.parse_lv_content,
            dont_filter = True
        )


    def parse_lv_content(self, response):
        """Parse content of LV"""

        self.total_count += 1
        if self.total_count >= 20000:
            if self.is_error_message(response):
                self.invalid_in_row += 1
            else:
                self.invalid_in_row = 0

        lv_item = {}
        lv_item['cislo_ku'] = self.ku_code
        lv_item['cislo_lv'] = response.meta['cislo_lv']
        # lv_item['ku'] = response.xpath(
        #     '//table[@summary="LV"]/tbody/tr[2]/td[2]/text()').extract_first()

        # owners
        # example: KU 733857, LV: 275
        # owners_table = response.xpath(
        #     '//table[@summary="Vlastníci, jiní oprávnění"]/tbody/tr')
        # owners_item.update(lv_item)
        # owners_item['vlastnici'] = []

        # for row in owners_table:
        #     # header check
        #     if row.xpath('th/text()').extract_first() is not None:
        #         continue

        #     owner = {}
        #     owner['vlastnik'] = row.xpath('td[1]/text()').extract_first()
        #     owner['podil'] = row.xpath('td[2]/text()').extract_first()

        #     owners_item['vlastnici'].append(owner)

        # grounds
        grounds_table = response.xpath('//table[@summary="Pozemky"]/tbody/tr')
        building_objects = 0
        grounds = 0
        for row in grounds_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            grounds += 1

            avalue = row.xpath('td/a/text()').extract_first()
            if 'součástí pozemku je stavba' in avalue:
                building_objects += 1

            # yield scrapy.Request(
            #     url,
            #     meta={
            #         'lv_item': lv_item
            #     },
            #     callback=self.parse_ground
            # )

        # buildings
        buildings_table = response.xpath('//table[@summary="Stavby"]/tbody/tr')
        buildings = 0
        for row in buildings_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            buildings += 1

            # building_item = {}
            # building_item['stavba'] = row.xpath('td/a/text()')
            # lv_item['stavby'].append(building_item)

            # yield scrapy.Request(
            #     url,
            #     meta={
            #         'lv_item': lv_item
            #     },
            #     callback=self.parse_building
            # )

        # units
        # example: KU 733857, LV 2000
        units_table = response.xpath('//table[@summary="Jednotky"]/tbody/tr')
        units = 0
        for row in units_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            units += 1

            # yield scrapy.Request(
            #     url,
            #     meta={
            #         'lv_item': lv_item
            #     },
            #     callback=self.parse_unit
            # )

        count_item = {
            'cislo_ku': self.ku_code,
            'cislo_lv': response.meta['cislo_lv'],
            'grounds': grounds,
            'building_objects': building_objects,
            'buildings': buildings,
            'units': units,
            'time': response.meta['download_latency']
        }

        if self.is_error_message(response):
            count_item['grounds'] = None
            count_item['building_objects'] = None
            count_item['buildings'] = None
            count_item['units'] = None

        yield count_item

        if self.invalid_in_row >= MAX_LV_NOT_FOUND_IN_ROW:
            return

        # yield scrapy.Request(
        #     START_URL,
        #     meta = {'cislo_lv': response.meta['cislo_lv'] + 1},
        #     callback=self.parse_again,
        #     dont_filter=True
        # )

        yield scrapy.FormRequest.from_response(
            self.ku_response,
            meta={'cislo_lv': response.meta['cislo_lv'] + 1},
            formdata={
                LV_INPUT_ELEMENT: str(response.meta['cislo_lv'] + 1),
                LV_SEARCH_BUTTON: SEARCH_TXT
            },
            callback=self.parse_lv_content,
            dont_filter = True
        )


    def parse_ground(self, response):
        if self.is_error_message(response):
            return

        # test_item = self.get_test_item(response)
        # test_item['request_type'] = 'pozemok'
        # yield test_item

        ground_item = response.meta['ground_item']

        # general data
        general_table = response.xpath(
            '//table[@summary="Atributy parcely"]/tbody/tr')
        for index, row in enumerate(general_table):
            name = {
                0: 'parcelni_cislo',
                1: 'obec',
                4: 'vymera',
                5: 'typ_parcely',
                8: 'druh_pozemku'
            }.get(index, None)

            if name is None:
                continue

            value = row.xpath('td[2]/text()').extract_first()
            ground_item[name] = value

        # building object
        building_object_item = {}
        ground_item['stavebni_objekt'] = building_object_item

        building_object_table = response.xpath(
            '//table[@summary="Atributy stavby"]/tbody/tr')
        for index, row in enumerate(building_object_table):
            name = row.xpath('td[1]/text()').extract_first()
            if name == 'Budova bez čísla popisného nebo evidenčního:':
                building_object_item['bez_cisel'] = True
                break

            if name == 'Stavební objekt:':
                ref = row.xpath('td[2]/a/@href').extract_first()
                yield scrapy.Request(
                    url,
                    meta={'building_object_item': building_object_item},
                    callback=self.parse_building_object
                )
                break

        # 'zpusoby ochrany nemovitosti'
        # parse_zom(self, response) - same for all subpages

        # BPEJ list
        # parse_bpej(self, response) - same for all subpages

        # 'omezeni vlastnickeho prava'
        # parse_ovp(self, response) - same for all subpages

        # other notes
        # parse_other_notes(self, response) - same for all subpages

        # operations - processing in another parse method
        # operation_refs = self.get_refs_from_detail_table(
        #     response, 'Řízení, v rámci kterých byl k nemovitosti zapsán cenový údaj')
        # for ref in operation_refs:
        #     yield scrapy.Request(ref, callback=self.parse_operation)

    def parse_building_object(self, response):
        building_object_item = response.meta['building_object_item']

        detail_table1 = response.xpath(
            '//table[@class="detail detail2columns"]/tbody/tr')
        for index, row in enumerate(detail_table1):
            name = {
                0: 'cisla_popisni_nebo_evidencni',
                1: 'typ',
                2: 'zpusob_vyuziti'
            }.get(index, None)

            if name is None:
                continue

            value = row.xpath('td[2]/text()').extract_first()
            building_object_item[name] = value

        detail_table2 = response.xpath('//table[@class="detail"]/tbody/tr')
        for index, row in enumerate(detail_table2):
            name = {
                0: 'datum_dokonceni',
                1: 'pocet_bytu',
                2: 'zastavena_plocha',
                4: 'podlahova_plocha',
                5: 'pocet_podlazi'
            }.get(index, None)

            if name is None:
                continue

            value = row.xpath('td[2]/text()').extract_first()
            building_object_item[name] = value

    def parse_building(self, response):
        if self.is_error_message(response):
            return

        building_item = response.meta['building_item']

        # general data
        building_table = response.xpath(
            '//table[@class="Atributy stavby"]/tbody/tr')
        for index, row in enumerate(building_table):
            name = row.xpath('td[1]/text()').extract_first()
            name = get_simple_string(name)
            value = row.xpath('td[2]/text()').extract_first()
            building_item[name] = value

        # test_item = self.get_test_item(response)
        # test_item['request_type'] = 'building detail opened'
        # yield test_item

        # 'zpusoby ochrany nemovitosti'
        # parse_zom(self, response) - same for all subpages

        # 'omezeni vlastnickeho prava'
        # parse_ovp(self, response) - same for all subpages

        # other notes
        # parse_other_notes(self, response) - same for all subpages

        # operations - processing in another parse method
        # operation_refs = self.get_refs_from_detail_table(
        #     response, 'Řízení, v rámci kterých byl k nemovitosti zapsán cenový údaj')
        # for ref in operation_refs:
        #     yield scrapy.Request(ref, callback=self.parse_operation)

    def parse_unit(self, response):
        if self.is_error_message(response):
            return

        # test_item = self.get_test_item(response)
        # test_item['request_type'] = 'unit detail opened'
        # yield test_item

        # TODO parsing data:
        # general data

        # 'zpusoby ochrany nemovitosti'
        # parse_zom(self, response) - same for all subpages

        # 'omezeni vlastnickeho prava'
        # parse_ovp(self, response) - same for all subpages

        # other notes
        # parse_other_notes(self, response) - same for all subpages

        # operations - processing in another parse method
        # operation_refs = self.get_refs_from_detail_table(
        #     response, 'Řízení, v rámci kterých byl k nemovitosti zapsán cenový údaj')
        # for ref in operation_refs:
        #     yield scrapy.Request(ref, callback=self.parse_operation)

    def parse_operation(self, response):
        if self.is_error_message(response):
            return

        # test_item = self.get_test_item(response)
        # test_item['request_type'] = 'operation detail opened'
        # yield test_item

    def get_refs_from_detail_table(self, response, table_name):
        table = response.xpath(
            '//table[@summary="{}"]/tbody/tr'.format(table_name))
        for row in table:
            refs = row.xpath('td/a/@href').extract()
            for ref in refs:
                url = urljoin(BASE_URL, ref)
                yield url

    def is_error_message(self, response):
        error_message = response.xpath(
            '//div[@id="ctl00_hlaseniOnMasterPage"]').extract_first()  # ctl00_updatePanelHlaseniOnMasterPage

        # TODO distinguish between 'not found' and 'session expired' message
        # print(error_message)
        return error_message is not None


def get_simple_string(str):
    """ Remove diacritics and punctation and replace spaces with underscores"""
    return unidecode(str).translate(None, string.punctuation)
