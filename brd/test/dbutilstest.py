__author__ = 'mouellet'

import unittest
import brd.db.dbutils as dbutils
import psycopg2

class TestDbUtils(unittest.TestCase):


    def setUp(self):
        self.dbconn = dbutils.get_connection()
        try:
            cr = self.dbconn.execute_transaction("create table test_1(id int, val varchar(20))")
            self.assertIsNone(cr, "Create table stmt returns None")
        except psycopg2.ProgrammingError, err:
            # this is the exception raised by psycopg2 when table already exist
            self.dbconn.execute_transaction("truncate table test_1")


    def tearDown(self):
        self.dbconn.execute_transaction("drop table test_1")


    def test_execute_and_fetch_all_transaction_ok(self):

        insert = "insert into test_1(id, val) values(%s, %s)"
        for i in range(10):
            params = (i, "val" + str(i))
            self.dbconn.execute_transaction(insert, params)
            self.assertEquals(psycopg2.extensions.TRANSACTION_STATUS_IDLE,
                              self.dbconn.get_connection().get_transaction_status(),
                              "Execute_transaction() should've committed, leaving no transaction active")

        # verify that rollback here has no impact
        self.dbconn.get_connection().rollback()

        res = self.dbconn.fetch_all_transaction("select * from test_1")
        self.assertEquals(psycopg2.extensions.TRANSACTION_STATUS_IDLE,
                              self.dbconn.get_connection().get_transaction_status(),
                              "Fecth_all_transaction() should've committed, leaving no current transaction active")

        self.assertEquals(10, len(res))
        self.assertEquals(0, res[0][0])
        self.assertEquals('val0', res[0][1])



    def test_execute_without_commit_ok(self):
        insert = "insert into test_1(id, val) values(%s, %s)"
        for i in range(10):
            params = (i, "val" + str(i))
            self.dbconn.execute(insert, params)
            self.assertEquals(psycopg2.extensions.TRANSACTION_STATUS_INTRANS,
                              self.dbconn.get_connection().get_transaction_status(),
                              "Execute_transaction() does NOT commit, so should leaving current transaction active")

        res = self.dbconn.fetch_all("select * from test_1")
        self.assertEquals(10, len(res))
        self.dbconn.get_connection().rollback()
        res = self.dbconn.fetch_all("select * from test_1")
        self.assertEquals(0, len(res), "Insert never committed, so the rollback should have removed all rows")



