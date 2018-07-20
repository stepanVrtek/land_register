import scrapy


class UrlItem(scrapy.Item):
    url = scrapy.Field()


class Nemovitosti(scrapy.Item):
    lv = scrapy.Field()
    ku = scrapy.Field()
