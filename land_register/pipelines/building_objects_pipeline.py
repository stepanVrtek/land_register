# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from land_register.pipelines.land_register_pipeline import (
    process_items_list,
    format_item
)


class BuildingObjectsPipeline:

    def process_item(self, item, spider):
        id_lv = item.get('id_lv')
        format_item(item['data'])

        spider.logger.info('Processing tem with ext_id_parcely: {}, lv id: {}'.format(
            item['data']['ext_id_parcely'], id_lv
        ))

        # in batch processing data are in list
        items = [item['data']]

        # just use function from land register pipeline
        process_items_list(id_lv, 'stavebni_objekt', items)

        return item
