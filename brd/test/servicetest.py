# -*- coding: utf-8 -*-
import shutil

import os

__author__ = 'mouellet'

import unittest
import brd.elt as db
import brd.service as service
import datetime
import brd.config as config

class TestService(unittest.TestCase):


    def setUp(self):
        self.dbconn = elt.get_connection()
        self.dbconn.execute_inTransaction("truncate staging.review cascade")
        self.dbconn.execute_inTransaction("truncate staging.load_audit cascade")


    def tearDown(self):
        pass
        # self.dbconn.execute_inTransaction("truncate staging.review cascade")
        # self.dbconn.execute_inTransaction("truncate staging.load_audit cascade")


    def test_bulk_load_scaped_data_ok(self):

        # pre-condition: dummy audit so bulk load does not fail with relational integrity (mock file have audit_id=2)
        now = datetime.datetime.now()
        a_id = brd.db.insert_auditing(job="MockScraped", step="Mockfiles", begin=datetime.date(2000, 1, 1), end=datetime.date(2001, 1, 1))
        self.dbconn.execute_inTransaction("update staging.load_audit set id = %s where id = %s ", (2, a_id))
        # fixture
        config.SCRAPED_OUTPUT_DIR = '/Users/mart/dev/p/brd/brd/test/mockscrapedfiles'
        config.SCRAPED_ARCHIVE_DIR ='/Users/mart/dev/p/brd/brd/test/mockscrapedfiles/archive'

        period = '1-1-2000_1-5-2010'
        n_treated, n_er = service.bulkload_review_files(period, remove_files=False)

        self.assertEqual(n_treated, 2)
        self.assertEqual(n_er, 1)
        # move back the expected one file from archive
        n = 0
        for l in os.listdir(config.SCRAPED_ARCHIVE_DIR):
            shutil.move(os.path.join(config.SCRAPED_ARCHIVE_DIR, l), config.SCRAPED_OUTPUT_DIR)
            n += 1
        self.assertEqual(n, 1)



