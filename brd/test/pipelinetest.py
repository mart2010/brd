__author__ = 'mouellet'

import unittest
import brd.db.dbutils as dbutils
import brd.scrapy.items as items
import brd.scrapy.pipelines.pipelines as pipelines
import psycopg2


class TestPipeline(unittest.TestCase):

    def setUp(self):
        self.dbconn = dbutils.get_connection()
        self.dbconn.execute_transaction("truncate staging.review")


    def tearDown(self):
        self.dbconn.execute_transaction("truncate staging.review")



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
        res = self.dbconn.fetch_all_transaction("select * from staging.review")
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



    def test_begin_and_end_period_load_ok(self):

        #TODO: based on Item date field and spider parse Date ...

        item_param = ReviewBaseItem()
        item_param['hostname'] = clibresSpider.allowed_domains[0]
        item_param['book_uid'] = "500"

