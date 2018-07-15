import scrapy


class MainPageSpider(scrapy.Spider):
    name = "mainScraper"

    start_url = ['http://nahlizenidokn.cuzk.cz/VyberLV.aspx']

    def parse(self, response):
        page = response.url.split("/")[-2]
        filename = 'quotes-%s.html' % page
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)