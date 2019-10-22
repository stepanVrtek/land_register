import scrapy
from land_register import db_handler


def generate_scraping_objects(ids):
    """Get next url."""
    db = db_handler.get_dataset()
    table = db['stavebni_objekt_ref'].table
    statement = table.select(table.c.id.in_(tuple(ids)))
    results = db.query(statement)

    for obj in results:
        yield obj


def update_scraping_object(scraping_object, status):
    db = db_handler.get_dataset()
    db['stavebni_objekt_ref'].update(dict(
        id=scraping_object['id'],
        stav_scrapingu=status
    ), ['id'])


class BuildingObjectsSpider(scrapy.Spider):
    """Scraping of building chosen building objects."""

    name = "BuildingObjectsSpider"
    custom_settings = {
        'ITEM_PIPELINES': {
            'land_register.pipelines.building_objects_pipeline.BuildingObjectsPipeline': 101
        }
    }

    def __init__(self, ids, **kwargs):
        self.scraping_objects = generate_scraping_objects(ids)

        super().__init__(**kwargs)

    def start_requests(self):
        scraping_object = next(self.scraping_objects)
        yield scrapy.Request(
            scraping_object['url'],
            meta={'scraping_object': scraping_object}
        )

    def parse(self, response):
        """Building object (stavebni objekt) parsing."""

        scraping_object = response.meta['scraping_object']

        building_object_item = {
            'id_lv': scraping_object['id_lv'],
            'item_type': 'STAVEBNI_OBJEKT',
            'data': {}
        }
        building_object_data = {
            'ext_id_parcely': scraping_object.get('ext_id_parcely'),
            'ext_id_stavebniho_objektu': scraping_object.get('ext_id_stavebniho_objektu')
        }

        # atributy
        detail_table1 = response.xpath(
            '(//table[@class="detail detail2columns"])[2]/tr')
        for index, row in enumerate(detail_table1):
            name = row.xpath('td[1]/text()').extract_first()
            name = {
                'Čísla popisná nebo evidenční:': 'cisla_popis_evid',
                'Typ:': 'typ',
                'Způsob využití:': 'zpusob_vyuziti'
            }.get(name)

            if name is None:
                continue

            value = row.xpath('td[2]/text()').extract_first()
            building_object_data[name] = value

        detail_table2 = response.xpath('//table[@class="detail"]/tr')
        for index, row in enumerate(detail_table2):
            name = {
                0: 'datum_dokonceni',
                1: 'pocet_bytu',
                2: 'zastavena_plocha',
                4: 'podlahova_plocha',
                5: 'pocet_podlazi'
            }.get(index)

            if name is None:
                continue

            value = row.xpath('td[2]/text()').extract_first()
            building_object_data[name] = value

        building_object_item['data'] = building_object_data
        yield building_object_item

        update_scraping_object(scraping_object, status='F')

        try:
            yield from self.start_requests()
            # next_scraping_object = next(self.scraping_objects)
            # yield scrapy.Request(
            #     scraping_object['url'],
            #     meta={'scraping_object': scraping_object}
            # )
        except StopIteration:
            pass

    def errback(self, failure):
        from scrapy.spidermiddlewares.httperror import HttpError
        from twisted.internet.error import DNSLookupError
        from twisted.internet.error import TimeoutError, TCPTimedOutError

        # log all failures
        self.logger.error(repr(failure))

        # in case you want to do something special for some errors,
        # you may need the failure's type:

        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)

    def response_is_ban(self, request, response):
        """Defines, which status codes mean ban."""
        return response.status == 403 or response.status == 500

    def closed(self, reason):
        """Called when spider is closed due to any error or success."""
        pass