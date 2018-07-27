# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
from scrapy.contrib.exporter import CsvItemExporter
import csv


class LandRegisterPipeline(object):
    def process_item(self, item, spider):
        return item


def write_to_csv(item):
    writer = csv.writer(open('KU_{}_LV_{}_export.csv'.format(item['ku_code'], item['lv_code']), 'a'), lineterminator='\n')
    writer.writerow([item[key] for key in item.keys()])

class WriteToCsv(object):
    def process_item(self, item, spider):
        write_to_csv(item)
        return item


# class CSVPipeline(object):

#     # def __init__(self):
#     #     pass

#     def close_spider(self, spider):
#         if self.exporter is not None:
#             self.exporter.finish_exporting()
#             self.file.close()

#     def process_item(self, item, spider):
#         self.file = open('KU_{}_LV_{}.csv'.format(item['ku_code'], item['lv_code']), 'w+b')
#         self.exporter = CsvItemExporter(self.file)
#         self.exporter.fields_to_export = ['request_type', 'download_latency', 'response_status', 'valid_request']
#         self.exporter.start_exporting()
#         self.exporter.export_item(item)
#         return item


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
#     file = open('%s_items.csv' % spider.name, 'w+b')
#     self.files[spider] = file
#     self.exporter = CsvItemExporter(file)
#     self.exporter.fields_to_export = ['request_type', 'download_latency', 'response_status', 'valid_request']
#     self.exporter.start_exporting()

#   def spider_closed(self, spider):
#     self.exporter.finish_exporting()
#     file = self.files.pop(spider)
#     file.close()

#   def process_item(self, item, spider):
#     self.exporter.export_item(item)
#     return item
