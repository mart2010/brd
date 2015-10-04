# -*- coding: utf-8 -*-
__author__ = 'mouellet'

import psycopg2
import brd.config as config
import uuid

# Note one psycopg2 transaction :
# Psycopg module and connection objects are thread-safe: many threads can access the same database either using separate sessions and creating
# a connection per thread or using the same connection and creating separate cursors. In DB API 2.0 parlance, Psycopg is level 2 thread safe.
# The difference between the above two approaches is that, using different connections, the commands will be executed in different sessions and
# will be served by different server processes. On the other hand, using many cursors on the same connection, all the commands will be executed
# in the same session (and in the same transaction if the connection is not in autocommit mode), but they will be serialized.




class DbConnection(object):
    """
    Class to allow for interacting with psycopg2, reusing psycopg2 heavyweight connection,
    managing transaction, ... etc.
    """
    def __init__(self, connection=config.DATABASE, readonly=False):
        self.connection = psycopg2.connect(host=connection['host'],
                                            database=connection['name'],
                                            port=connection['port'],
                                            user=connection['user'],
                                            password=connection['pwd'])
        if readonly:
            self.connection.set_session(readonly=readonly)


    def execute_inTransaction(self, sql, params=None):
        """
        Execute sql statement as a single transaction
        :return rowcount impacted
        """
        # connection context manager: if no exception raised within block then transaction is committed (otherwise rolled back)
        with self.connection as c:
            # cursor context manager : will close/release any resource held by cursor (ex. result cache)
            with c.cursor() as curs:
                curs.execute(sql, params)
                return curs.rowcount

    def execute(self, sql, params=None):
        """
        Execute sql statement while leaving open the transaction.
        :return rowcount impacted
        """
        with self.connection.cursor() as curs:
            curs.execute(sql, params)
            return curs.rowcount


    def insert_row_get_id(self, insert, params=None):
        """
        Insert a single row while leaving open the transaction.
        :param sql:
        :param params:
        :return: the auto-generated id
        """
        insert_sql = insert + " RETURNING id"
        with self.connection.cursor() as curs:
            curs.execute(insert_sql, params)
            return curs.fetchone()[0]


    def fetch_one(self, sql, params=None):
        """
        Execute sql query and fetch one row
        :param sql:
        :param params:
        :return: fetched row
        """
        with self.connection.cursor() as curs:
            curs.execute(sql, params)
            return curs.fetchone()


    def fetch_all_inTransaction(self, query, params=None):
        """
        Execute query, fetch all records into a list and return it as a single transaction.
        """
        with self.connection as c:
            with c.cursor() as curs:
                curs.execute(query, params)
                result = curs.fetchall()
                return result


    def fetch_all(self, query, params=None):
        """
        Execute query, fetch all records into a list and return it while leaving open the transaction.
        """
        with self.connection.cursor() as curs:
            curs.execute(query, params)
            result = curs.fetchall()
            return result


    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()


    def __del__(self):
        self.connection.close()

    def __str__(self):
        return self.connection.__str__()



# impl tentative for Conn Obj that can chain CTAS stmt...
class CTAStage(object):
    temptables = []

    def __init__(self, connection=config.DATABASE):
        self._connection = psycopg2.connect(host=connection['host'],
                                            database=connection['name'],
                                            port=connection['port'],
                                            user=connection['user'],
                                            password=connection['pwd'])
        self._cursor = self._connection.cursor()

    def stage(self, sql, params):
        """ Create a new temporary table with CTAS stmt
        :param sql: select stmt used as source, with '%(tmp)s' indicating the previous temp stage table
        :param params: name params stored as dict
        :return: self
        """

        if len(self.temptables) != 0:
            params['tmp'] = self.temptables[-1]

        ctas = 'CREATE TEMP TABLE %(newtmp) AS ' + sql
        tablename = str(uuid.uuid4())
        params['newtmp'] = tablename
        self._cursor.execute(sql, params)
        self.temptables.append(tablename)
        return self

    def insert(self, insertsql, params):
        """ Insert into an existing target table using sql with insert stmt and sub-select
        :param insertsql: insert stmt and sub-select with '%(tmp)s' indicating the previous temp stage table
        :param params: name params stored as dict
        :return self
        """
        # assert(len(self.temptables) > 0, "Can only insert from previously created temp table")
        self._cursor.execute(insertsql, params)
        self._connection.commit()
        return self


# possible usage
# stmt1 = "sssss"
# stmt2 = "dddd"
# ctas = CTAStage(get_conn_param()).stage(stmt1, {'datemin' : '12-03-2011'})\
#                                 .stage(stmt2, {})\
#                                     .stage(stmt1, {})\
#                                         .insert(stmt2, {})




# Singleton connections handy for client code that can share same connection
# All commands will be run withon same session and serialized
# may not have a good use-case in this app ?
conn_readonly = None
def get_ro_connection():
    global conn_readonly
    if conn_readonly is None:
        conn_readonly = DbConnection(readonly=True)
    return conn_readonly

conn = None
def get_connection():
    global conn
    if conn is None:
        conn = DbConnection(readonly=False)
    return conn





















