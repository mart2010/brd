# -*- coding: utf-8 -*-
import datetime
import json
import uuid
import psycopg2
import psycopg2.extras
import brd.config as config


__author__ = 'mouellet'


class EltStepStatus():
    FAILED = 'Failed'
    COMPLETED = 'Completed'
    RUNNING = 'currently running..'


class DbConnection(object):
    """
    Class to allow for interacting with psycopg2, reusing psycopg2 heavyweight connection,
    managing transaction, ... etc.
    """

    def __init__(self, connection, readonly=False, autocommit=False):
        self.connection = psycopg2.connect(**connection)
        self.connection.set_session(readonly=readonly, autocommit=autocommit)


    def execute_inTransaction(self, sql, params=None):
        """
        Execute sql statement as a single transaction
        :return rowcount impacted
        """
        # connection context manager: if no exception raised in context then trx is committed (otherwise rolled back)
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

    def copy_into_table(self, schematable, columns, open_file, delim='|'):
        """
        Execute copy_expert
        :return rowcount impacted
        """
        sql = \
            """
            copy %s( %s )
            from STDIN with csv HEADER DELIMITER '%s' NULL '';
            """ % (schematable, columns, delim)

        with self.connection.cursor() as curs:
            curs.copy_expert(sql, open_file, size=8192)
            return curs.rowcount

    def fetch_one(self, sql, params=None):
        """
        Execute sql query and fetch one row
        :param sql:
        :param params:
        :return: fetched row
        """
        with self.connection.cursor() as curs:
            curs.execute(sql, params)
            one_row = curs.fetchone()
        return one_row

    def fetch_all(self, query, params=None, in_trans=False, as_dict=False):
        """
        Execute query, return all records as a list of tuple (or as dictionary)
        leaving open the transaction if in_trans=True or commit otherwise.
        """
        if as_dict:
            cur = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            cur = self.connection.cursor()
        cur.execute(query, params)
        result = cur.fetchall()
        cur.close()
        if in_trans:
            self.connection.commit()
        return result

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def __del__(self):
        self.connection.close()

    def __str__(self):
        return self.connection.__str__()


# convenient fcts taken from my DbConnection
def insert_row_get_id(connection, insert, params=None):
        """
        Insert a single row while leaving open the transaction.
        :return: the auto-generated id
        """
        if insert.rfind(";") == -1:
            insert += ' RETURNING id;'
        else:
            insert = insert.replace(';', ' RETURNING id;')

        with connection.cursor() as curs:
            curs.execute(insert, params)
            return curs.fetchone()[0]




# Singleton Dbconnection on default database
conn_readonly = None

def get_ro_connection():
    global conn_readonly
    if conn_readonly is None:
        # autocommit=True to be sure no transaction started (even for select)
        conn_readonly = DbConnection(connection=config.DATABASE, readonly=True, autocommit=True)
    return conn_readonly

conn = None

def get_connection():
    global conn
    if conn is None:
        conn = DbConnection(connection=config.DATABASE)
    return conn


def insert_auditing(batch_job, step, connection=None, commit=True, **named_params):
    sql = \
        """
        insert into staging.load_audit(batch_job, step_name, status, run_dts)
                            values (%(job)s, %(step)s, %(status)s, %(run_dts)s);
        """
    if connection:
        c = connection
    else:
        c = get_connection().connection

    named_params['job'] = batch_job
    named_params['step'] = step
    now = datetime.datetime.now()
    named_params['run_dts'] = now
    named_params['status'] = named_params.get('status', EltStepStatus.RUNNING)

    audit_id = insert_row_get_id(c, sql, named_params)
    if commit:
        c.commit()
    return (audit_id, now)


def update_auditing(audit_id, status, run_dts=None, connection=None, commit=True, **named_params):
    sql = \
        """
        update staging.load_audit set status = %(status)s
                                    ,rows_impacted = %(rows)s
                                    ,elapse_sec = %(elapse_sec)s
                                    ,output = %(output)s
        where id = %(id)s;
        """
    if connection:
        c = connection
    else:
        c = get_connection().connection

    named_params['id'] = audit_id
    named_params['status'] = status
    named_params['rows'] = named_params.get('rows')
    named_params['output'] = named_params.get('output')
    now = datetime.datetime.now()
    if run_dts:
        named_params['elapse_sec'] = (now - run_dts).seconds
    else:
        named_params['elapse_sec'] = None
    c.cursor().execute(sql, named_params)
    if commit:
        c.commit()


def bulkload_file(filepath, schematable, column_headers, period):
    """
    Bulkload filepath into schematable and manage auditing
    :param filepath: full path of file
    :param schematable: format expected 'schema.table'
    :param column_headers:
    :param period: period associated to file for auditing
    :return: (audit_id, #ofRow)  Note: #ofRow = -1, in case of error
    """
    now = datetime.datetime.now()
    audit_id = insert_auditing(job='Bulkload file', step=filepath, begin=period[0], end=period[1], start_dts=now)
    try:
        with open(filepath) as f:
            count = get_connection().copy_into_table(schematable, column_headers, f)
        _update_auditing(commit=True, rows=count, status=EltStepStatus.COMPLETED, id=audit_id,
                        elapse_sec=(datetime.datetime.now()-now).seconds)
        print("Bulkload of %s completed (audit-id: %s, #OfLines: %s" %(filepath, audit_id, count))
        return (audit_id, count)
    except psycopg2.DatabaseError, er:
        get_connection().rollback()
        insert_auditing(commit=True, job='Bulkload file', step=filepath, rows=-1, status=EltStepStatus.FAILED,
                        comment=er.pgerror, begin=period[0], end=period[1], start_dts=now)
        # also add logging
        print("Error bulk loading file: \n'%s'\nwith msg: '%s' " % (filepath, er.message))
        return (audit_id, -1)


def truncate_table(schema_table, commit=False):
    sql = "truncate table %s.%s;" % (schema_table['schema'], schema_table['table'])
    get_connection().execute(sql)
    if commit:
        get_connection().commit()


class EltError(Exception):
    def __init__(self, message, cause=None):
        self.message = message
        self.cause = cause

    def __str__(self):
        return self.message


class CTAStage(object):
    """
    Tentative for defining CTAS chained-steps, possible usage:
        stmt1 = "sssss"
        stmt2 = "dddd"
        ctas = CTAStage(get_conn_param()).stage(stmt1, {'datemin' : '12-03-2011'})\
                                   .stage(stmt2, {})\
                                     .stage(stmt1, {})\
                                        .insert(stmt2, {})

    """
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
