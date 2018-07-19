import scrapy
from pprint import pprint
from scrapy.crawler import CrawlerProcess

class LandRegisterListSpider(scrapy.Spider):
    name = "Land Register List Spider"
    start_urls = ['http://nahlizenidokn.cuzk.cz/VyberLV.aspx']
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': True
    }

    def parse(self, response):
        # print(response.xpath('//input[@id="__VIEWSTATE"]/@value').extract_first())

        yield scrapy.FormRequest.from_response(
            response,
            formdata = {
                'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$txtKU': '600016',
                'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$btnKU': 'Vyhledat'
            },
            callback=self.parse_second
        )

    def parse_second(self, response):
        ku_xpath = '//span[@id="ctl00_bodyPlaceHolder_vyberObecKU_vyberKU_lblKU"]/text()'
        print(response.xpath(ku_xpath).extract_first())

        yield scrapy.FormRequest.from_response(
            response,
            formdata = {
                'ctl00$bodyPlaceHolder$txtLV': '1',
                'ctl00$bodyPlaceHolder$btnVyhledat': 'Vyhledat'
            },
            callback = self.parse_content
        )

    def parse_content(self, response):
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


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(LandRegisterListSpider)
process.start()
