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
            print(url)
            yield scrapy.Request(url, callback=self.parse_ground)

    def parse_ground(self, response):
        if self.is_error_message(response):
            return

        print("successful ground request")


    def is_error_message(self, response):
        error_message = response.xpath(
            '//div[@id="ctl00_hlaseniOnMasterPage"]').extract_first() # ctl00_updatePanelHlaseniOnMasterPage
        print(error_message)
        return error_message is not None



process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(LandRegisterListSpider)
process.start()
