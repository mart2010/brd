__author__ = 'mouellet'

import psycopg2
import ConfigParser


def get_config():
    conf = ConfigParser.SafeConfigParser()
    conf.read('../config.conf')
    return conf

class DbConnection(object):
    """
    This allows reusing psycopg2 heavyweight connection.  Use read-only mode to avoid any
    sort of database lock done?? by session (all cursors created are executed in the same session,
    and long query could cause concurrency issue)
    """
    def __init__(self, conf, readonly):
        try:
            self._connection = psycopg2.connect(host= conf.get('Database','host'),
                                database= conf.get('Database','name'),
                                port=conf.get('Database','port'),
                                user=conf.get('Database','user'),
                                password=conf.get('Database','pwd') )
            if readonly:
                self._connection.set_session(readonly= readonly)
        except Exception, msg:
            # do some logging and send alert
            raise Exception("Database connection error: " + msg)

    def getConnection(self):
        return self._connection

    def query(self, query, params):
        """Cursor are lightweight and created as often as needed (each time method called)
        (note: cursors used to fetch result cache data and use memory according to result set size)
        """
        cursor = self._connection.cursor()
        return cursor.execute(query, params)

    def __del__(self):
        self._connection.close()

    def __str__(self):
        return self._connection.__str__()


import uuid

# impl tentative for Conn Obj that can chain CTAS stmt...
class CTAStage(DbConnection):
    temptables = []

    def __init__(self, conf):
        DbConnection.__init__(self, conf, False )
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
        assert(len(self.temptables) > 0, "Can only insert from previously created temp table")
        self._cursor.execute(insertsql, params)
        self._connection.commit()
        return self


# the usage

stmt1 = "sssss"
stmt2 = "dddd"
ctas = CTAStage(get_config()).stage(stmt1, {'datemin' : '12-03-2011'})\
                                .stage(stmt2, {})\
                                    .stage(stmt1, {})\
                                        .insert(stmt2, {})




CONN_READONLY = None
def get_ro_connection():
    global CONN_READONLY
    if CONN_READONLY is None:
        CONN_READONLY = DbConnection(get_config(), True)
    return CONN_READONLY

CONN = None
def get_connection():
    global CONN
    if CONN is None:
        CONN = DbConnection(get_config(), False)
    return CONN



def fetch_critiqueslibres_stat():
    squery = """select title, nb_reviews
                from staging.critiques
             """
    result = {}
    for row in get_ro_connection().query(squery):
        result[row[0]] = row[1]
    return result













