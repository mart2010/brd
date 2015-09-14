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
    def __init__(self, conn=config.DATABASE, readonly=False):
        self._connection = psycopg2.connect(host=conn['host'],
                                            database=conn['name'],
                                            port=conn['port'],
                                            user=conn['user'],
                                            password=conn['pwd'])
        if readonly:
            self._connection.set_session(readonly=readonly)

    def get_connection(self):
        return self._connection


    def execute_transaction(self, sql, params=None):
        """
        Execute sql statement as a single transaction.
        """
        # connection context manager: if no exception raised within block then transaction is committed (otherwise rolled back)
        with self._connection as c:
            # cursor context manager : will close/release any resource held by cursor (ex. result cache)
            with c.cursor() as curs:
                print "about to execute " + sql + ", with params:" + str(params)
                curs.execute(sql, params)

    def execute(self, sql, params=None):
        """
        Execute sql statement and return result while leaving open the transaction.
        """
        with self._connection.cursor() as curs:
            curs.execute(sql, params)


    def fetch_all_transaction(self, query, params=None):
        """
        Execute query, fetch all records into a list and return it as a single transaction.
        """
        with self._connection as c:
            with c.cursor() as curs:
                curs.execute(query, params)
                result = curs.fetchall()
                return result


    def fetch_all(self, query, params=None):
        """
        Execute query, fetch all records into a list and return it while leaving open the transaction.
        """
        with self._connection.cursor() as curs:
            curs.execute(query, params)
            result = curs.fetchall()
            return result


    def commit(self):
        self._connection.commit()

    def __del__(self):
        self._connection.close()

    def __str__(self):
        return self._connection.__str__()



# impl tentative for Conn Obj that can chain CTAS stmt...
class CTAStage(object):
    temptables = []

    def __init__(self, conn=config.DATABASE):
        self._connection = psycopg2.connect(host=conn['host'],
                                            database=conn['name'],
                                            port=conn['port'],
                                            user=conn['user'],
                                            password=conn['pwd'])
        self._cursor = self._connection.cursor()

    def stage(self, sql, params):
        """ Create a new temporary table with CTAS stmt
        :param sql: select stmt used as source, with '%(tmp)s' indicating the previous temp stage table
        :param params: name params stored as dict
        :return: self
        """

        # By convention, the %(tmp) is the previously created table
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
        :param sql: insert stmt and sub-select with '%(tmp)s' indicating the previous temp stage table
        :param params: name params stored as dict
        :return: self
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















