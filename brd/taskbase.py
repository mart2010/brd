import brd
import luigi
from luigi import six
__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The BRD Project"

# defined in Batch entry point Task (shared among all tasks)
batch_name = "n.a."  # concurrent batch jobs should be launched in separate process


def postgres_target(target_table, update_id):
    return luigi.postgres.PostgresTarget(
            host        =brd.config.DATABASE['host'],
            database    =brd.config.DATABASE['database'],
            user        =brd.config.DATABASE['user'],
            password    =brd.config.DATABASE['password'],
            table       =target_table,
            update_id   =update_id)

class BasePostgresTask(luigi.Task):
    """
    Provides to subclass Task function to write to DB target
    as valid luigi's task.  Also manages the audit-metadata.

    Subclass must provide target table (self.table) and
    implements sql logic in exec_sql().
    """

    def output(self):
        return postgres_target(self.table, self.task_id)

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()

        # decorate with audit-log stuff
        self.audit_id, self.run_dts = brd.elt.insert_auditing(batch_name, self.task_id)
        self.rowscount = self.exec_sql(cursor, self.audit_id)
        cursor.close()

        # mark as complete in same transaction (checkpoint)
        self.output().touch(connection)
        # commit and clean up
        connection.commit()
        connection.close()

    def on_success(self):
        brd.elt.update_auditing(self.audit_id, brd.elt.EltStepStatus.COMPLETED,
                                run_dts=self.run_dts, rows=self.rowscount)

    def on_failure(self, exception):
        brd.elt.update_auditing(self.audit_id, brd.elt.EltStepStatus.FAILED,
                                run_dts=self.run_dts, output=str(exception))

    def exec_sql(self, cursor, audit_id):
        raise NotImplementedError


class BaseBulkLoadTask(luigi.postgres.CopyToTable):
    """
    Provides to subclass Task function to bulkload file to DB target
    as valid luigi's task.  Also manages the audit-metadata.

    Subclass must provide target table, columns, column_separator and
    implements requires() logic.
    """
    # (cannot use postgre_target() as attributes set as abstractproperty in rdbms.CopyToTable)
    host = brd.config.DATABASE['host']
    database = brd.config.DATABASE['database']
    user = brd.config.DATABASE['user']
    password = brd.config.DATABASE['password']

    clear_table_before = False
    # default separator
    column_separator = '|'
    # container of values inserted as NULL values (MO: added empty string)
    null_values = (None, "")
    # added to manage col headers
    input_has_headers = False

    def requires(self):
        raise NotImplementedError

    def __init__(self, *args, **kwargs):
        super(BaseBulkLoadTask, self).__init__(*args, **kwargs)
        self.run_dts = None
        self.audit_id = None
        self.rowscount = None

    def init_copy(self, connection):
        if self.clear_table_before:
            connection.cursor().execute('truncate table %s;' % self.table)

    def run(self):

        # if file not empty, read 1st line (header)
        header = None
        with self.input().open('r') as fobj:
            try:
                header = fobj.next()
            # avoid executing downstream Task for empty file
            except StopIteration, e:
                raise ImportError("File empty, task %s is stopped" % self.task_id)

        if self.input_has_headers and header:
            self.columns = header.strip('\n').split(self.column_separator)

        # decorate with audit-log stuff
        self.audit_id, self.run_dts = brd.elt.insert_auditing(batch_name, self.task_id)
        super(BaseBulkLoadTask, self).run()


    # the override copy() is needed to handle file with col headers (and return rowscount)
    # TODO: suggest to add to luigi:  accept headers and populate columns based on these..
    def copy(self, cursor, file):
        if isinstance(self.columns[0], six.string_types):
            column_names = self.columns
        elif len(self.columns[0]) == 2:
            column_names = [c[0] for c in self.columns]
        else:
            raise Exception('columns must consist of column strings or (column string, type string) tuples (was %r ...)' % (self.columns[0],))

        if self.input_has_headers:
            self.rowscount = self.copy_expert(file, cursor)
        else:
            cursor.copy_from(file, self.table, null=r'\\N', sep=self.column_separator, columns=column_names)
            self.rowscount = cursor.rowcount

    def copy_expert(self, infile, cursor):
        """
        Execute copy_expert
        :return rowcount impacted
        """
        # removed: "NULL '%s'"  and use default NULL (empty and \N) since luigi generated tmp file
        # does not produce the \\N correctly)
        sql = \
            """
            copy %s( %s )
            from STDIN with csv HEADER DELIMITER '%s'; """ \
            % (self.table, ",".join(self.columns), self.column_separator)

        cursor.copy_expert(sql, infile, size=8192)
        return cursor.rowcount

    def on_success(self):
        if self.audit_id:
            brd.elt.update_auditing(self.audit_id, brd.elt.EltStepStatus.COMPLETED,
                                    run_dts=self.run_dts, rows=self.rowscount)

    def on_failure(self, exception):
        if self.audit_id:
            brd.elt.update_auditing(self.audit_id, brd.elt.EltStepStatus.FAILED,
                                    run_dts=self.run_dts, output=str(exception))
