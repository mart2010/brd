# -*- coding: utf-8 -*-
__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The BRD Project"

from scrapy.exceptions import DropItem
from scrapy import signals
from scrapy.exporters import CsvItemExporter



class DumpToFile(object):
    """
    Dump harvested data into flat file, no other logic is implemented here
    (it's "Dump" :-)
    """
    def __init__(self):
        self.files = {}
        self.counter = 0

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        # TODO: verify if still needed for registration of spider_closed/opened event?
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        filename = spider.get_dump_filepath()
        f = open(filename, 'w')
        self.files[spider.name] = f
        self.exporter = CsvItemExporter(f, include_headers_line=True, delimiter='|')
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        f = self.files.pop(spider.name)
        f.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        # for counter, could set att in spider at closing
        self.counter += 1
        return item



