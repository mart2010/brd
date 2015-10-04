# -*- coding: utf-8 -*-

__author__ = 'mouellet'

import unittest
import brd.db.dbutils as dbutils
import brd.scrapy.items as items
import brd.scrapy.pipelines.pipelines as pipelines
import brd.scrapy.spiders.reviewspiders as reviewspiders
from scrapy.exceptions import DropItem
import psycopg2
from datetime import datetime

class TestPipeline(unittest.TestCase):

    def setUp(self):
        self.dbconn = dbutils.get_connection()
        self.dbconn.execute_inTransaction("truncate staging.review")


    def tearDown(self):
        self.dbconn.execute_inTransaction("truncate staging.review")



    def test_load_item_with_all_mandatory_fields_ok(self):

        pipeline_loader = pipelines.ReviewStageLoader()
        item = items.ReviewBaseItem()
        # set all mandatory fields
        item['hostname'] = "thehost"
        item['reviewer_pseudo'] = "thepseudo"
        item['review_rating'] = "therating"
        item['review_date'] = "12 dec 2013"
        item['book_title'] = "theBook"
        item['book_lang'] = "FR"

        retitem = pipeline_loader.process_item(item, None)
        self.assertEqual(item, retitem)

        # call commit by closing manually the spider
        pipeline_loader.close_spider(None)

        # check-out the DB data
        res = self.dbconn.fetch_all_inTransaction("select * from staging.review")
        self.assertEquals(1, len(res))

        for r in item.values():
            self.assertTrue(r in res[0])



    def test_load_item_with_missing_fields_fail(self):

        pipeline_loader = pipelines.ReviewStageLoader()

        item = items.ReviewBaseItem()
        # set only one field
        item['hostname'] = "thehost"
        with self.assertRaises(psycopg2.IntegrityError):
            pipeline_loader.process_item(item, None)



    def test_review_filter_is_ok(self):

        scraper = reviewspiders.CritiquesLibresSpider(begin_period='01/01/2010', end_period='01/02/2010')
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
        self.assertEquals(ret_item['derived_review_date'], datetime.strptime('01/01/2010', '%d/%m/%Y'))
        self.assertEquals(ret_item['derived_title_sform'], "THE,-BOOK")


        item['review_date'] = u"31 janvier 2010"
        ret_item = pipeline_loader.process_item(item, scraper)
        self.assertEquals(ret_item['derived_review_date'], datetime.strptime('31/01/2010', '%d/%m/%Y'))



