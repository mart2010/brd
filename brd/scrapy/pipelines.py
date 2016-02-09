# -*- coding: utf-8 -*-

from scrapy.exceptions import DropItem
from scrapy import signals
from scrapy.exporters import CsvItemExporter


class ReviewParser(object):
    """
    This pipeline is responsible in parsing some raw fields (review_date, parsed_rating)
    """

    def process_item(self, item, spider):
        # spider knows how to parse its date text, rating...
        review_date = spider.parse_review_date(item['review_date'])
        review_rating = spider.parse_rating(item['rating'])
        item['parsed_review_date'] = review_date
        item['parsed_rating'] = review_rating
        return item


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



