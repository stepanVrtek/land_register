import scrapy
from urllib.parse import urljoin
from pprint import pprint
from datetime import datetime


START_URL = 'https://nahlizenidokn.cuzk.cz/VyberRizeni.aspx'

class WorpkplacesSpider(scrapy.Spider):
    name = "WorkplacesSpider"
    start_urls = [START_URL]

    def parse(self, response):
        self.parse_workplaces(response)

    def parse_workplaces(self, response):
        wp_table = response.xpath(
            '//select[@name="ctl00$bodyPlaceHolder$vyberPracoviste$listPracoviste"]/*')
            # //option[not(@disabled="Disabled") and not(contains(text(),"zrušeno"))]""")

        self.workplaces = []
        for row in wp_table:
            if row.xpath('@disabled').extract_first() == "Disabled":
                continue

            optiongroup = row.xpath('option')
            label = row.xpath('@label').extract_first()
            if optiongroup:
                for option in optiongroup:
                    self.prepare_workplace(option, label)
            else:
                self.prepare_workplace(row, None)

        self.save_workplaces()

    def prepare_workplace(self, option, label):
        cislo_pracoviste = int(option.xpath('@value').extract_first())
        nazev_pracoviste = option.xpath('text()').extract_first()

        if 'zrušeno' in nazev_pracoviste:
            return

        self.workplaces.append((
            cislo_pracoviste,
            nazev_pracoviste,
            label
        ))

    def save_workplaces(self):
        values = ', '.join(map(str, self.workplaces)
            ).replace('None', "'NULL'")
