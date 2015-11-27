# -*- coding: utf-8 -*-
import shutil
import os

__author__ = 'mouellet'

import unittest
import brd.db.dbutils as dbutils
import brd.db.service as service
import psycopg2
import datetime
import brd.config as config

class TestService(unittest.TestCase):


    def setUp(self):
        self.dbconn = dbutils.get_connection()
        self.dbconn.execute_inTransaction("truncate staging.review cascade")
        self.dbconn.execute_inTransaction("truncate staging.load_audit cascade")


    def tearDown(self):
        pass
        #self.dbconn.execute_inTransaction("truncate staging.review cascade")
        #self.dbconn.execute_inTransaction("truncate staging.load_audit cascade")


    def test_bulk_load_scaped_data_ok(self):

        # pre-condition: dummy audit log so bulk load does not fail with relational integrity (mock file have audit_id=2)
        now = datetime.datetime.now()
        a_id = service.load_auditing({'job': "MockScraped", 'step': "Mockfiles", 'begin': now, 'end': now})
        self.dbconn.execute_inTransaction("update staging.load_audit set id = %s where id = %s ", (2, a_id))
        # fixture
        config.SCRAPED_OUTPUT_DIR = '/Users/mouellet/dev/p/brd/brd/test/mockscrapedfiles'
        config.SCRAPED_ARCHIVE_DIR ='/Users/mouellet/dev/p/brd/brd/test/mockscrapedfiles/archive'

        period = '1-1-2000_1-5-2010'
        n_treated, n_er = service.bulkload_review_files(period, remove=False)

        self.assertEqual(n_treated, 2)
        self.assertEqual(n_er, 1)
        # move back the expected one file from archive
        n = 0
        for l in os.listdir(config.SCRAPED_ARCHIVE_DIR):
            shutil.move(os.path.join(config.SCRAPED_ARCHIVE_DIR, l), config.SCRAPED_OUTPUT_DIR)
            n += 1
        self.assertEqual(n, 1)



