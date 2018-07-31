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

class TitleDeedListSpider(scrapy.Spider):
    name = "TitleDeedListSpider"

    def __init__(self, ku_code = '', **kwargs):
        self.ku_code = ku_code

        # self.change_proxy()
        self.overall_counter = 0
        self.proxy_counter = 0
        self.invalid_in_row = 0

        super().__init__(**kwargs)

    start_urls = [START_URL]

    def response_is_ban(self, request, response):
        return response.status == 403

    def get_test_item(self, response):
        item = TestItem()
        item['ku_code'] = self.ku_code
        item['lv_code'] = str(response.meta.get('lv_code', 'none'))
        item['download_latency'] = response.meta['download_latency']
        item['response_status'] = response.status
        item['valid_request'] = True
        return item

    def parse(self, response):
        """Parse KU code (kod katastralneho uzemia)"""

        test_item = self.get_test_item(response)
        test_item['request_type'] = 'first page loading'
        yield test_item

        yield scrapy.FormRequest.from_response(
            response,
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

        self.ku_response = response

        yield scrapy.FormRequest.from_response(
            response,
            meta = {'lv_code': 1},
            formdata = {
                LV_INPUT_ELEMENT: '1',
                LV_SEARCH_BUTTON: SEARCH_TXT
            },
            callback = self.parse_content
        )

    def parse_content(self, response):
        """Parse content of LV"""
        test_item = self.get_test_item(response)
        test_item['request_type'] = 'LV input'

        if self.is_error_message(response):
            test_item['valid_request'] = False
            self.invalid_in_row += 1
        else:
            self.invalid_in_row = 0

        yield test_item

        if self.invalid_in_row >= 100:
            return

        next_lv_code = response.meta['lv_code'] + 1

        yield scrapy.FormRequest.from_response(
            self.ku_response,
            meta = {'lv_code': next_lv_code},
            formdata = {
                LV_INPUT_ELEMENT: str(next_lv_code),
                LV_SEARCH_BUTTON: SEARCH_TXT
            },
            callback = self.parse_content
        )



    def is_error_message(self, response):
        error_message = response.xpath(
            '//div[@id="ctl00_hlaseniOnMasterPage"]').extract_first() # ctl00_updatePanelHlaseniOnMasterPage

        # TODO distinguish between 'not found' and 'session expired' message

        return error_message is not None
