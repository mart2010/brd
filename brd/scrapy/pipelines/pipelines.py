# -*- coding: utf-8 -*-

# Define item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import brd.db.dbutils as dbutils
import brd.scrapy.utils as utils

class ReviewFilterAndConverter:
    """
    This pipeline is responsible in filtering out the review out of period and
    to parse/convert some field (ex. derived_title_sform, derived_review_date)
    """

    def __init__(self):
        self.begin_period = None
        self.end_period = None

    def open_spider(self, spider):
        self.begin_period = spider.begin_period
        self.end_period = spider.end_period

    def process_item(self, item, spider):
        # spider knowns how to parse date from raw string
        review_date = spider.parse_date(item['review_date'])

        # manage review_date
        if review_date < self.begin_period or self.end_period <= review_date:
            pass
        else:
            item['derived_review_date'] = review_date

        # manage title_sform
        item['derived_title_sform'] = utils.convert_to_sform(item['book_title'])











class ReviewStageLoader(object):

    sql_insert_review = 'insert into staging.review(%s) values (%s)'
    sql_insert_reviewer = 'insert into staging.reviewer(%s) values (%s)'


    def __init__(self):
        """ The pipeline instantiates dedicate connection to manage transaction explicitly
        """
        self.db_conn = dbutils.DbConnection()


    def process_item(self, item, spider):
        # here I will adjust insert_sql depending whether spider is of type Reviews, Reviewer, ..
        self.insert_data(item, self.sql_insert_review)
        # process_item() must return item as specified by contract (for downstream consumption)
        return item

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
        print("hey Im' calling commit with spider:" + str(spider))
        self.db_conn.commit()



