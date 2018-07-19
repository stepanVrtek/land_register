import scrapy
from pprint import pprint
from scrapy.crawler import CrawlerProcess


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
                KU_INPUT_ELEMENT: '6000564',
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
        owners = response.xpath('//table[@summary="Vlastníci, jiní oprávnění"]/tbody/tr')

        items = []
        for row in owners:
            # header check
            if row.xpath('th/text()').extract_first() is not None:
                continue

            items.append({
                'vlastnik': row.xpath('td[1]/text()').extract_first(),
                'podil': row.xpath('td[2]/text()').extract_first()
            })

        print(items)

    def is_error_message(self, response):
        error_message = response.xpath(
            '//div[@id="ctl00_updatePanelHlaseniOnMasterPage"]').extract_first()
        return error_message is not None



process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(LandRegisterListSpider)
process.start()
