# -*- coding: utf-8 -*-

__author__ = 'mouellet'

import unittest
import brd.db.dbutils as dbutils
import psycopg2

class TestDbUtils(unittest.TestCase):


    def setUp(self):
        self.dbconn = dbutils.get_connection()
        try:
            cr = self.dbconn.execute_inTransaction("create table test(id int, val varchar(20))")
            self.assertEqual(-1, cr, "Create table stmt should have returned -1")
        except psycopg2.ProgrammingError, err:
            print str(err)


    def tearDown(self):
        self.dbconn.execute_inTransaction("drop table if exists test")


    def test_execute_and_fetch_all_transaction_ok(self):

        insert = "insert into test(id, val) values(%s, %s)"
        for i in range(10):
            params = (i, "val" + str(i))
            ret = self.dbconn.execute_inTransaction(insert, params)
            self.assertEqual(1, ret)
            self.assertEquals(psycopg2.extensions.TRANSACTION_STATUS_IDLE,
                              self.dbconn.connection.get_transaction_status(),
                              "Commit expected, leaving no transaction active")

        # verify that rollback here has no impact
        self.dbconn.rollback()

        res = self.dbconn.fetch_all_inTransaction("select * from test")
        self.assertEquals(psycopg2.extensions.TRANSACTION_STATUS_IDLE,
                          self.dbconn.connection.get_transaction_status(),
                          "Commit expected, leaving no transaction active")

        self.assertEquals(10, len(res))
        self.assertEquals(0, res[0][0])
        self.assertEquals('val0', res[0][1])



    def test_execute_without_commit_ok(self):
        insert = "insert into test(id, val) values(%s, %s)"
        for i in range(10):
            params = (i, "val" + str(i))
            ret = self.dbconn.execute(insert, params)
            self.assertEqual(1, ret)
            self.assertEquals(psycopg2.extensions.TRANSACTION_STATUS_INTRANS,
                              self.dbconn.connection.get_transaction_status(),
                              "Execute_transaction() does NOT commit, so should leaving current transaction active")

        res = self.dbconn.fetch_all("select * from test")
        self.assertEquals(10, len(res))
        self.dbconn.rollback()
        res = self.dbconn.fetch_all("select * from test")
        self.assertEquals(0, len(res), "Insert never committed, so the rollback should have removed all rows")


    def test_insert_and_fetch_id_ok(self):
        self.dbconn.execute("drop table if exists auto_table")
        self.dbconn.execute("create table auto_table(id serial, t varchar(10))")
        id = self.dbconn.insert_row_and_fetch_id("insert into auto_table(t) values('ttt')")
        self.assertEquals(1, id)
        self.assertTrue(type(id) == int)
        self.dbconn.execute("drop table auto_table")


    def test_fetch_one(self):
        # TODO: implement-me
        pass

