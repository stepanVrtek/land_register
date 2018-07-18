import scrapy
from pprint import pprint
from scrapy.crawler import CrawlerProcess

class LandRegisterListSpider(scrapy.Spider):
    name = "Land Register List Spider"
    start_urls = ['http://nahlizenidokn.cuzk.cz/VyberLV.aspx']

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
        print(response.xpath('//span[@id="ctl00_bodyPlaceHolder_vyberObecKU_vyberKU_lblKU"]/text()').extract())

        yield scrapy.FormRequest.from_response(
            response,
            formdata = {
                'ctl00$bodyPlaceHolder$txtLV': '1',
                'ctl00%24bodyPlaceHolder%24txtLV=1&ctl00%24bodyPlaceHolder%24btnVyhledat': 'Vyhledat'
            },
            callback = self.parse_land_register
        )

    def pase_land_register(self, response):
        print(response)



process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(LandRegisterListSpider)
process.start()
