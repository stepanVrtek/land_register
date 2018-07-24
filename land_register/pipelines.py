# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
from scrapy.contrib.exporter import CsvItemExporter


class LandRegisterPipeline(object):
    def process_item(self, item, spider):
        return item


class CSVPipeline(object):
    def __init__(self):
        self.exporters = []

    def close_spider(self, spider):
        for e in self.exporters:
            e.finish_exporting()
            self.file.close()

    def process_item(self, item, spider):
        self.file = open('KU_{}_LV_{}.csv'.format(item['ku_code'], item['lv_code']), 'w+b')
        self.files[spider] = file
        exporter = CsvItemExporter(file)
        exporter.fields_to_export = ['request_type', 'download_latency', 'response_status', 'valid_request']
        exporter.start_exporting()
        exporter.export_item(item)
        self.exporters.append(e)
        return item


# class CSVPipeline(object):

#   def __init__(self):
#     self.files = {}

#   @classmethod
#   def from_crawler(cls, crawler):
#     pipeline = cls()
#     crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
#     crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
#     return pipeline

#   def spider_opened(self, spider):
#     file = open('exports/%s_items.csv' % spider.name, 'w+b')
#     self.files[spider] = file
#     self.exporter = CsvItemExporter(file)
#     self.exporter.fields_to_export = ['ku_code', 'lv_code', 'request_type', 'download_latency', 'response_status', 'valid_request']
#     self.exporter.start_exporting()

#   def spider_closed(self, spider):
#     self.exporter.finish_exporting()
#     file = self.files.pop(spider)
#     file.close()

#   def process_item(self, item, spider):
#     self.exporter.export_item(item)
#     return item