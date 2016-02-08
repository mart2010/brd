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


def execute_and_get_rowcount(connection, sql, params=None):
    """
    Execute sql statement while leaving open the transaction.
    :return rowcount impacted
    """
    with connection.cursor() as curs:
        curs.execute(sql, params)
        return curs.rowcount



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


# class BatchProcessor(object):
#     """
#     Responsible in executing Steps in Batch AND manage auditing metadata!
#
#     Step in self.steps list are executed in order and may be some sql stmt or yet be
#     some kind of callable object (i.e. function or class with __call__ attribute)
#     """
#     def __init__(self, name, period=None):
#         """
#
#         :param name:
#         :param period: (begin_period, end_period)
#         :param dependencies: list of schema.table and filepath
#         :return:
#         """
#         self.name = name
#         if period:
#             self.begin_period = period[0]
#             self.end_period = period[1]
#         self.steps = list()
#         # use default read/write connection
#         self.conn = get_connection()
#
#     def define_period(self, period):
#         self.begin_period = period[0]
#         self.end_period = period[1]
#
#     def execute_prerequisite_step(self):
#         """
#         Some batch requires prerequisite step(s) to be done before all other steps.
#         A prerequisite's step has negative step_no and return its output (eg. flatfile generated)
#         :return: output produced by this step (and stored in load_audit.output)
#         """
#         # assert(self.prerequisite_step)
#         # TODO: check if there is a prerequisite step completed from a previous failed Batch
#
#         now = datetime.datetime.now()
#         self.prerequisite_step.named_params['audit_id'] = \
#                 _insert_auditing(job=self.name, step=self.prerequisite_step.name,
#                                  step_no=self.prerequisite_step.stepno, start_dts=now,
#                                  begin=self.begin_period, end=self.end_period)
#         # execute step's function or sql
#         if self.prerequisite_step.is_callable:
#             ret = self.prerequisite_step.obj_callable(**self.prerequisite_step.named_params)
#         else:
#             ret = self.conn.execute(self.prerequisite_step.sql, self.prerequisite_step.named_params)
#
#         _update_auditing(insert_dts=now, commit=True, output=ret, status=EltStepStatus.COMPLETED,
#                          id=self.prerequisite_step.named_params['audit_id'])
#
#         return ret
#
#
#
#
#     def add_step(self, step):
#         """
#         This is to add regular step which can be run sequentially without
#         needing any parameter value at runtime (except the audit_id)
#         :param step:
#         :return:
#         """
#         step.set_stepno(len(self.steps) + 1)
#         self.steps.append(step)
#
#
#     def _check_dependencies(self):
#         """
#         Check table/filepath exist and not empty
#         :return
#         """
#         sql = \
#             """
#             select count(*)
#             from %s
#             """ % self.table
#         res = self.conn.fetch_one(sql)
#         pass
#         # TODO
#         # raise EltError("Batch '%s' depend on an empty resource: %s" % (self.batch_name, tableorfile))
#
#     def execute_batch(self):
#         """
#         Execute all steps or starting from a previously failed steps (if found in load_audit)
#         and update the audit-log info accoringly
#         :param period:
#         :return:
#         """
#         last_step_no, last_status = self.get_last_audit_steps()
#
#         if last_status == EltStepStatus.RUNNING:
#             raise EltError("Batch '%s' is still running" % self.name)
#
#         if last_status == EltStepStatus.FAILED:
#             # skip already completed step
#             self.steps = self.steps[last_step_no - 1:]
#
#         # execute batch
#         self._process_generic_steps()
#         print ("Finished processing Batch '%s!" % self.name)
#
#     def _process_generic_steps(self):
#         """
#         :param period: (d-m-yyyy, d-m-yyyy)
#         """
#         # reset/rollback pending trx (RESET and SET SESSION AUTHORIZATION reset session to default)
#         # self.conn.connection.reset()
#         try:
#             for step in self.steps:
#                 now = datetime.datetime.now()
#                 step.named_params['audit_id'] = _insert_auditing(job=self.name, step=step.name,
#                                                                  step_no=step.stepno, start_dts=now,
#                                                                  begin=self.begin_period, end=self.end_period)
#                 # execute step's function or sql
#                 if step.is_callable:
#                     ret = step.obj_callable(**step.named_params)
#                 else:
#                     ret = self.conn.execute(step.sql, step.named_params)
#
#                 if step.is_auditid_required():
#                     step.add_auditid_toname()
#                 _update_auditing(insert_dts=now, commit=True, rows=ret, status=EltStepStatus.COMPLETED,
#                                  step=step.name, id=step.named_params['audit_id'])
#         except Exception, e:
#             conn.rollback()
#             msg = EltStepStatus.FAILED + ": Batch failed at step '%s' with error: '%s'" % (step.name, e.message)
#             _insert_auditing(commit=True, job=self.name, status=EltStepStatus.FAILED,
#                              comment=msg, step=step.name, step_no=step.stepno,
#                              start_dts=now, begin=self.begin_period, end=self.end_period)
#             raise EltError(msg, e)
#
#     def get_last_audit_steps(self):
#         """
#         Check status of last step executed for Batch_name logged in load_audit
#         :return: (step_no, status) of last step or (-1, Completed) when no record found
#         """
#         sql_last_step = \
#             """
#             select step_no, status
#             from staging.load_audit as l
#             where batch_job = %s
#             and id = (select max(id) from staging.load_audit where batch_job = l.batch_job);
#             """
#         resp = self.conn.fetch_one(sql_last_step, (self.name,))
#         if resp is None:
#             return (-1, EltStepStatus.COMPLETED)
#
#         last_step_no, last_status = resp
#         if last_status.startswith(EltStepStatus.COMPLETED):
#             last_status = EltStepStatus.COMPLETED
#         elif last_status.startswith(EltStepStatus.FAILED):
#             last_status = EltStepStatus.FAILED
#         elif last_status.startswith(EltStepStatus.RUNNING):
#             last_status = EltStepStatus.RUNNING
#         else:
#             raise EltError(
#                 "Step no%d of Batch '%s' has invalid status '%s'" % (last_step_no, self.name, last_status))
#         return (last_step_no, last_status)
#
#     def __str__(self):
#         return "Batch '%s' with steps: %s " % (self.name, str(self.steps))
#


def insert_auditing(connection, batch_job, step, **named_params):
    sql = \
        """
        insert into staging.load_audit(batch_job, step_name, status, run_dts)
                            values (%(job)s, %(step)s, %(status)s, %(run_dts)s);
        """
    named_params['job'] = batch_job
    named_params['step'] = step
    now = datetime.datetime.now()
    named_params['run_dts'] = now
    named_params['status'] = named_params.get('status', EltStepStatus.RUNNING)
    audit_id = insert_row_get_id(connection, sql, named_params)
    return (audit_id, now)


def update_auditing(connection, audit_id, status, run_dts=None, **named_params):
    sql = \
        """
        update staging.load_audit set status = %(status)s
                                    ,rows_impacted = %(rows)s
                                    ,elapse_sec = %(elapse_sec)s
                                    ,output = %(output)s
        where id = %(id)s;
        """
    named_params['id'] = audit_id
    named_params['status'] = status
    named_params['rows'] = named_params.get('rows')
    named_params['output'] = named_params.get('output')
    now = datetime.datetime.now()
    if run_dts:
        named_params['elapse_sec'] = (now - run_dts).seconds
    else:
        named_params['elapse_sec'] = None
    connection.cursor().execute(sql, named_params)


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
