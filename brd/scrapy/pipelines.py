# -*- coding: utf-8 -*-

# Define item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
from scrapy import signals
from scrapy.exporters import CsvItemExporter


class ReviewFilter(object):
    """
    This pipeline is responsible of
    1) getting the parsed review_date field
    1) filtering out Reviews not within load period

    N.B. no business-rule transfo allowed (these would imply new scraping whenever rules change!)
    """

    def __init__(self):
        self.begin_period = None
        self.end_period = None

    def open_spider(self, spider):
        self.begin_period = spider.begin_period
        self.end_period = spider.end_period

    def process_item(self, item, spider):
        # spider knowns how to parse its date text
        review_date = spider.parse_review_date(item['review_date'])

        # Keep only reviews within period
        if self.begin_period <= review_date < self.end_period:
            item['parsed_review_date'] = review_date
        else:
            raise DropItem("Review outside loading period")
        return item



class DumpToFile(object):
    """
    Dump harvested data into flat file, no other logic like managing audit metadata or file naming
    is implemented here (it's "Dump" :-)

    # Similar to Feed-Exporters used with: scrapy crawl spider_name -o output.csv -t csv
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
        filename = spider.dump_filepath
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
        # counter is left, however it could set an att in spider during closing (but difficult to get back considering the way service triggers spider)
        self.counter += 1
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



