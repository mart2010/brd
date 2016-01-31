# -*- coding: utf-8 -*-
import datetime
import uuid
import psycopg2
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

    def insert_row_get_id(self, insert, params=None):
        """
        Insert a single row while leaving open the transaction.
        :param sql:
        :param params:
        :return: the auto-generated id
        """
        if insert.rfind(";") == -1:
            insert += ' RETURNING id;'
        else:
            insert = insert.replace(';', ' RETURNING id;')

        with self.connection.cursor() as curs:
            curs.execute(insert, params)
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
            one_row = curs.fetchone()
        return one_row

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
        Execute query, return all records as a list of tuple (or None) while leaving open the transaction.
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


# Singleton Dbconnection on default database
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


class Step(object):
    def __init__(self, name, sql, named_params=None):
        self.name = name
        self.sql = sql
        if named_params is None:
            self.named_params = {}
        else:
            self.named_params = named_params
        self.stepno = -1

    def set_stepno(self, stepno):
        self.stepno = stepno

    def __str__(self):
        return self.name


class BatchProcessor(object):
    """
    Used to define/execute Batch composed of a number of ordered steps (sql stmts) to execute
    and sourced from same source_table (ex. load_staged_reviews, load_thingisbn, ..).

    Commit is done after each step executed successfully, and after failure only failed steps
    are re-executed (starting from failed step).

    Steps are defined in list of 'Step' object, each containing sql and params to execute
    """

    def __init__(self, batch_name, source_table):
        self.batch_name = batch_name
        self.source_table = source_table
        self.steps = list()
        # use default read/write connection
        self.conn = get_connection()

    def add_step(self, step):
        step.set_stepno(len(self.steps) + 1)
        self.steps.append(step)

    def execute_batch(self):
        """
        Execute all steps of the batch (or starting from a previously failed steps)
        while updating the audit-log info
        :param period:
        :return:
        """
        last_step_no, last_status = self._get_last_audit_steps()

        if last_status == EltStepStatus.RUNNING:
            raise EltError("Batch '%s' is still running, wait before launching a new one" % self.batch_name)

        period_in_stage = self._fetch_periods_data_in_stage()
        if last_status == EltStepStatus.FAILED:
            if period_in_stage is None:
                raise EltError("Previous Batch '%s' has failed but %s is empty" % (self.batch_name, self.source_table))
            # skip already completed step
            self.steps = self.steps[last_step_no - 1:]
        elif last_status == EltStepStatus.COMPLETED:
            if period_in_stage is None:
                raise EltError("Cannot launch new Batch '%s' stage.%s is empty" % (self.batch_name, self.source_table))
        # execute batch
        self._process_generic_steps(period_in_stage)
        print ("Finished processing Batch '%s!" % self.batch_name)

    def _process_generic_steps(self, period):
        """
        :param period: (d-m-yyyy, d-m-yyyy)
        """
        # reset/rollback pending trx (RESET and SET SESSION AUTHORIZATION reset session to default)
        # self.conn.connection.reset()
        period_begin, period_end = period

        try:
            for step in self.steps:
                now = datetime.datetime.now()
                step.named_params['audit_id'] = insert_auditing(job=self.batch_name,
                                                                step=step.name,
                                                                step_no=step.stepno,
                                                                start_dts=now,
                                                                begin=period_begin, end=period_end)
                # execute step's sql
                nb_rows = get_connection().execute(step.sql, step.named_params)
                update_auditing(commit=True,
                                rows=nb_rows,
                                status=EltStepStatus.COMPLETED,
                                finish_dts=datetime.datetime.now(),
                                id=step.named_params['audit_id'])
        except psycopg2.DatabaseError, dbe:
            conn.rollback()
            msg = EltStepStatus.FAILED + ": Batch failed at step '%s' with DB error: '%s'" % (step.name, dbe.message)
            insert_auditing(commit=True,
                            job=self.batch_name,
                            status=EltStepStatus.FAILED,
                            comment=msg,
                            step=step.name,
                            step_no=step.stepno,
                            start_dts=now,
                            begin=period_begin, end=period_end)
            raise EltError(msg, dbe)

    def _fetch_periods_data_in_stage(self):
        """
        Fetch all periods found in staging.tablename
        :return (min begin, max end) or None when table empty
        """
        sql = \
            """
            select min(period_begin), max(period_end)
            from staging.%s r
            join staging.load_audit a on (a.id = r.load_audit_id);
            """ % self.source_table
        res = get_ro_connection().fetch_one(sql)
        return res

    def _get_last_audit_steps(self):
        """
        :return: (step_no, status) of last step log-audit for Batch
        or else (-1, Completed) when no log-audit found
        """
        sql_last_step = \
            """
            select step_no, status
            from staging.load_audit as l
            where batch_job = %s
            and id = (select max(id) from staging.load_audit where batch_job = l.batch_job);
            """
        resp = get_ro_connection().fetch_one(sql_last_step, (self.batch_name,))

        if resp is None:
            return (-1, EltStepStatus.COMPLETED)

        last_step_no, last_status = resp
        if last_status.startswith(EltStepStatus.COMPLETED):
            last_status = EltStepStatus.COMPLETED
        elif last_status.startswith(EltStepStatus.FAILED):
            last_status = EltStepStatus.FAILED
        elif last_status.startswith(EltStepStatus.RUNNING):
            last_status = EltStepStatus.RUNNING
        else:
            raise EltError(
                "Step no%d for batch '%s' has invalid status '%s'" % (last_step_no, self.batch_name, last_status))
        return (last_step_no, last_status)

    def __str__(self):
        return "Batch '%s' with these steps: %s " % (self.batch_name, str(self.steps))


def insert_auditing(commit=False, **named_params):
    sql = \
        """
        insert into staging.load_audit(batch_job, step_name, step_no, status, comment,
                                       rows_impacted, period_begin, period_end, start_dts)
                            values (%(job)s, %(step)s, %(step_no)s, %(status)s,%(comment)s,
                                    %(rows)s, %(begin)s, %(end)s, %(start_dts)s);
        """
    assert ('job' in named_params)
    assert ('step' in named_params)
    assert ('begin' in named_params)
    assert ('end' in named_params)
    assert ('start_dts' in named_params)
    named_params['status'] = named_params.get('status', EltStepStatus.RUNNING)
    named_params['step_no'] = named_params.get('step_no', 0)
    named_params['rows'] = named_params.get('rows', -1)
    named_params['comment'] = named_params.get('comment')
    ret = get_connection().insert_row_get_id(sql, named_params)
    if commit:
        get_connection().commit()
    return ret


def update_auditing(commit=False, **named_params):
    sql = \
        """
        update staging.load_audit set status = %(status)s
                                    ,rows_impacted = %(rows)s
                                    ,finish_dts = %(finish_dts)s
        where id = %(id)s;
        """
    assert ('status' in named_params)
    assert ('rows' in named_params)
    assert ('id' in named_params)
    assert ('finish_dts' in named_params)
    ret = get_connection().execute(sql, named_params)
    if commit:
        get_connection().commit()
    return ret


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
        update_auditing(commit=True, rows=count, status=EltStepStatus.COMPLETED, id=audit_id,
                        finish_dts=datetime.datetime.now())
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
