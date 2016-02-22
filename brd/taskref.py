import brd
import brd.config
import brd.scrapy
import brd.service
import luigi
import luigi.parameter
import luigi.postgres
import os
from brd.taskbase import BasePostgresTask, BaseBulkLoadTask, batch_name


class DownLoadThingISBN(luigi.Task):
    filepath = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filepath)


class BulkLoadThingISBN(BaseBulkLoadTask):
    """
    This task bulkload thingisbn file into staging
    TODO: truncate staging table first not to accumulate older reference
    """
    filename = luigi.Parameter()
    table = 'staging.THINGISBN'
    columns = ["WORK_REFID", "ISBN_ORI", "ISBN10", "ISBN13"]

    def requires(self):
        fullpath_name = os.path.join(brd.config.REF_DATA_DIR, self.filename)
        return DownLoadThingISBN(fullpath_name)

    def init_copy(self, connection):
        # clear staging thingisbn before!
        connection.cursor().execute('truncate table %s;' % self.table)


class LoadWorkRef(BasePostgresTask):
    filename = luigi.Parameter()
    table = 'integration.work'

    def requires(self):
        return BulkLoadThingISBN(self.filename)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work(refid, create_dts, load_audit_id)
            select distinct s.work_refid, now(), %(audit_id)s
            from staging.thingisbn s
            left join integration.work w on s.work_refid = w.refid
            where w.refid is null;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount


class LoadIsbnRef(BasePostgresTask):
    filename = luigi.Parameter()
    table = 'integration.isbn'

    def requires(self):
        return BulkLoadThingISBN(self.filename)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.isbn(ean, isbn13, isbn10, create_dts, load_audit_id)
            select distinct cast(s.isbn13 as bigint), s.isbn13, s.isbn10, now(), %(audit_id)s
            from staging.thingisbn s
            left join integration.isbn i on cast(s.isbn13 as bigint) = i.ean
            where i.ean is null;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount

# TODO: what about possible deletion of work_refid/isbn (relevant also for work and isbn table)
class LoadWorkIsbnRef(BasePostgresTask):
    filename = luigi.Parameter()
    table = 'integration.work_isbn'

    def requires(self):
        return [LoadWorkRef(self.filename), LoadIsbnRef(self.filename)]

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work_isbn(ean, work_refid, source_site_id, create_dts, load_audit_id)
            select distinct cast(s.isbn13 as bigint), s.work_refid, (select id from integration.site where logical_name = 'librarything')
                    , now(), %(audit_id)s
            from staging.thingisbn s
            left join integration.work_isbn i on (cast(s.isbn13 as bigint) = i.ean and s.work_refid = i.work_refid)
            where i.ean is null;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount


class BatchLoadWorkRef(luigi.Task):
    """
    Entry point to launch loads related to 'thingisbn.csv' reference file
    """
    filename = luigi.Parameter(default='thingISBN.csv')
    global batch_name
    batch_name = "Ref thingisbn"  # for auditing

    def requires(self):
        return [LoadWorkIsbnRef(self.filename)]

