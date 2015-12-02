# -*- coding: utf-8 -*-

__author__ = 'mouellet'

import unittest
from datetime import datetime

import brd.db.dbutils as dbutils
import brd.scrapy.items as items
import brd.scrapy.pipelines as pipelines
import brd.scrapy.spiders.reviewspiders as reviewspiders
from scrapy.exceptions import DropItem
import psycopg2
import brd.config as config


class TestPipeline(unittest.TestCase):

    def setUp(self):
        self.dbconn = dbutils.get_connection()
        self.dbconn.execute_inTransaction("truncate staging.review")
        self.dbconn.execute_inTransaction("truncate staging.load_audit")


    def tearDown(self):
        self.dbconn.execute_inTransaction("truncate staging.review")
        self.dbconn.execute_inTransaction("truncate staging.load_audit")


    def test_dump_item_into_flatfile_ok(self):

        # dependencies fixture
        spider = reviewspiders.CritiquesLibresSpider(begin_period='1-1-2001', end_period='31-12-2015')
        config.SCRAPED_OUTPUT_DIR = '/Users/mouellet/dev/p/brd/brd/test/mockscrapedfiles/'

        pipeline_loader = pipelines.DumpScrapedData()
        pipeline_loader.spider_opened(spider)

        item = items.ReviewBaseItem()
        # set all mandatory fields
        item['hostname'] = "thehost"
        item['reviewer_pseudo'] = "thepseudo"
        item['review_rating'] = "therating"
        item['review_date'] = "12 dec 2013"
        item['book_title'] = "theBook"
        item['book_lang'] = "FR"


        retitem = pipeline_loader.process_item(item, spider)
        self.assertEqual(item['hostname'], retitem['hostname'])
        self.assertTrue(len(retitem.items()) == 7)
        self.assertTrue(int(retitem['load_audit_id']) > 0)

        # finishing scrappig by closing manually the spider
        pipeline_loader.spider_closed(spider)

        # checkout the generated file
        fn = pipeline_loader.get_dump_filename(spider)
        gf = open(config.SCRAPED_OUTPUT_DIR + fn)
        line = gf.readline()
        self.assertTrue(line.index('|') > 0)
        line = gf.readline()
        self.assertTrue(line.index('|') > 0)
        line = gf.readline()
        self.assertTrue(line == '')
        gf.close()

        # check-out the DB audit data
        res = self.dbconn.fetch_all_inTransaction("select * from staging.load_audit")
        self.assertEquals(1, len(res))


    # no longer useful
    def t_load_item_with_missing_fields_fail(self):

        pipeline_loader = pipelines.ReviewStageLoader()

        item = items.ReviewBaseItem()
        # set only one field
        item['hostname'] = "thehost"
        with self.assertRaises(psycopg2.IntegrityError):
            pipeline_loader.process_item(item, None)



    def test_review_filter_is_ok(self):

        scraper = reviewspiders.CritiquesLibresSpider(begin_period='1-1-2010', end_period='1-2-2010')
        pipeline_loader = pipelines.ReviewFilterAndConverter()
        pipeline_loader.open_spider(scraper)

        item = items.ReviewBaseItem()
        item['book_title'] = " the, Book "
        item['review_date'] = u"31 décembre 2009"

        with self.assertRaises(DropItem):
            pipeline_loader.process_item(item, scraper)

        item['review_date'] = u"1 février 2010"
        with self.assertRaises(DropItem):
            pipeline_loader.process_item(item, scraper)


        item['review_date'] = u"1 janvier 2010"
        ret_item = pipeline_loader.process_item(item, scraper)
        self.assertEquals(ret_item['book_title'], item['book_title'])
        # check out derived attrs
        self.assertEquals(ret_item['derived_review_date'], datetime.strptime('1-1-2010', '%d-%m-%Y'))
        self.assertEquals(ret_item['derived_title_sform'], "THE,-BOOK")


        item['review_date'] = u"31 janvier 2010"
        ret_item = pipeline_loader.process_item(item, scraper)
        self.assertEquals(ret_item['derived_review_date'], datetime.strptime('31-01-2010', '%d-%m-%Y'))



