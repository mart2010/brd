# -*- coding: utf-8 -*-

# Define item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from brd import config
import brd.scrapy.scrapy_utils as scrapy_utils
import brd.utils as utils
from scrapy.exceptions import DropItem
from scrapy import signals
from scrapy.exporters import CsvItemExporter
import brd.db.service as service


class ReviewFilterAndConverter(object):
    """
    This pipeline is responsible of
    1) filtering out Reviews not within load period
    2) adding derived fields (ex. derived_title_sform, derived_review_date)
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
        item['derived_title_sform'] = scrapy_utils.convert_book_title_to_sform(item['book_title'])
        return item




class DumpScrapedData(object):
    """
    Dump scraped data into flat file and log it into load_audit metadata.
    # Could also be done by Feed-Exporters with no extra-code (scrapy crawl spider_name -o output.csv -t csv)
    # but is less integrated with the code base (output setting must be redefined...)

    """

    def __init__(self):
        self.files = {}
        self.audit = {}
        self.counter = 0

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        # TODO: is this needed to do somekind of registration of spider_closed/opened event?
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline


    def spider_opened(self, spider):
        filename = self.get_dump_filename(spider)
        f = open(config.SCRAPED_OUTPUT_DIR + filename, 'w')
        self.files[spider.name] = f
        self.exporter = CsvItemExporter(f, include_headers_line=True, delimiter='|')
        # audit record must have correct period (used for filtering when loading staging.reviews)
        step = "Loaded file: " + filename
        audit_id = service.insert_auditing(job=DumpScrapedData.__name__,
                                           step=step,
                                           begin=spider.begin_period,
                                           end=spider.end_period)
        self.audit[spider.name] = audit_id
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        f = self.files.pop(spider.name)
        f.close()
        service.update_auditing(commit=True,
                                rows=self.counter,
                                status="Completed",
                                id=self.audit[spider.name])


    def process_item(self, item, spider):
        item['load_audit_id'] = self.audit[spider.name]
        self.exporter.export_item(item)
        self.counter += 1
        return item

    def update_audit(self, nb_rows, audit_id):
        service.update_auditing(rows=nb_rows, status="Completed", id=audit_id)

    def get_dump_filename(self, spider):
        return "ReviewOf" + spider.name + '_' + \
               utils.get_period_text(spider.begin_period, spider.end_period) + '.dat'



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



