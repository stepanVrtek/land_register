import scrapy
from urllib.parse import urljoin
from pprint import pprint
from datetime import datetime

START_URL = 'https://cica.vugk.sk/LV_vyber.aspx'


class SlovakCadasterSpider(scrapy.Spider):
    name = "SlovakCadasterSpider"
    start_urls = [START_URL]

    def parse(self, response):
        yield scrapy.FormRequest.from_response(
            response,
            formdata = {
                'ScriptManager': 'UpdatePanel1|DropDownList_okres',
                'DropDownList_okres': 'Banská Štiavnica',
                'DropDownList_ku': 'Baďan',
                'DropDownList_LV': '0',
                'DropDownList_obec': 'BAĎAN' ,
                '__EVENTTARGET': 'DropDownList_okres',
                '__ASYNCPOST': 'true'
            },
            callback = self.ku_select
        )

    def ku_select(self, response):
        ku_table = response.xpath(
            '//select[@name="DropDownList_ku"]').extract()

        pprint(ku_table)
        pprint(response.body.decode("utf-8"))
