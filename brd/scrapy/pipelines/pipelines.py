# -*- coding: utf-8 -*-

# Define item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from brd import config
import brd.scrapy.utils as utils
from scrapy.exceptions import DropItem
from scrapy import signals
import csv
from scrapy.exporters import CsvItemExporter


class ReviewFilterAndConverter(object):
    """
    This pipeline is responsible of
    1) filtering out Reviews not within load period
    2) parsing/converting some fields (ex. derived_title_sform, derived_review_date)
    """

    def __init__(self):
        self.begin_period = None
        self.end_period = None

    def open_spider(self, spider):
        self.begin_period = spider.begin_period
        self.end_period = spider.end_period

    def process_item(self, item, spider):
        # spider knowns how to parse its date raw string
        review_date = spider.parse_review_date(item['review_date'])

        # manage review_date
        if self.begin_period <= review_date < self.end_period:
            item['derived_review_date'] = review_date
        else:
            raise DropItem("Review outside loading period")

        # manage title_sform
        item['derived_title_sform'] = utils.convert_to_sform(item['book_title'])
        return item




class CsvExporter(object):
    """
    Dump scraped data into flat file
    # Could be done by Feed-Exporters with no extra-code (scrapy crawl spider_name -o output.csv -t csv)
    # but is less integrated with the code base (output setting must be redefined...)

    """

    def __init__(self):
        self.files = {}

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        # TODO: is this needed to do someking of registration of spider_closed/opened event?
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def get_filename(self, prefix, begin_period, end_period):
        b = str(begin_period.day) + '-' + str(begin_period.month) + '-' + str(begin_period.year)
        e = str(end_period.day) + '-' + str(end_period.month) + '-' + str(end_period.year)
        return prefix + '_' + b + '_' + e + '.dat'


    def spider_opened(self, spider):
        fn = config.SCRAPED_OUTPUT_DIR + self.get_filename(spider.name, spider.begin_period, spider.end_period)
        f = open(fn, 'w')
        self.files[spider] = f
        self.exporter = CsvItemExporter(f, include_headers_line=True, delimiter='|')
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        f = self.files.pop(spider)
        f.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item









class OldIdeaToLoadDB(object):

    def insert_data(self, item, insert_sql):
        keys = item.fields.keys()
        fields = ','.join(keys)
        params = ','.join(['%s'] * len(keys))
        sql = insert_sql % (fields, params)

        # TODO:
        # add technical field (TODO: checkout the AsIS('paramvale') for the now())
        # keys['loading_dts'] = 'now()'

        # missing scraped value should return None (result in inserting Null)
        values = [item.get(k, None) for k in keys]

        self.db_conn.execute(sql, values)

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        # commit once spider has finished scraping
        self.db_conn.commit()



