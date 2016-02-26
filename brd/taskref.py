import json
import logging

import datetime

import brd
import brd.config
import brd.scrapy
import brd.service as service
import luigi
import luigi.postgres
import os
from brd.taskbase import BasePostgresTask, BaseBulkLoadTask, batch_name


# ----------------------------------  LOAD WORK_REF ------------------------------------------#
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
    clear_table_before = True

    def requires(self):
        fullpath_name = os.path.join(brd.config.REF_DATA_DIR, self.filename)
        return DownLoadThingISBN(fullpath_name)


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


# --------------------------------------------------------------------------------------------- #
# ----------------------------------  LOAD WORK_INFO  ----------------------------------------- #
# --------------------------------------------------------------------------------------------- #
class FetchWorkIdsWithoutInfo(luigi.Task):
    """
    This fetches n_work having NO work_info harvested, while avoiding duplicate
    work-ids (to avoid trying many times same work through its duplicate)
    """
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()

    def output(self):
        wids_filepath = '/tmp/wids_forworkinfo_%s.txt' % \
                        (self.harvest_dts.strftime(luigi.DateMinuteParameter.date_format))
        return luigi.LocalTarget(wids_filepath)

    def run(self):
        f = self.output().open('w')
        res_dic = service.fetch_workIds_no_info(self.n_work)
        nb_available = len(res_dic)
        if nb_available == 0:
            raise Exception("No more work without info was found, stop process!")
        elif nb_available < self.n_work:
            logging.info("Only %d ids found without info (out of %d requested)" % (nb_available, int(self.n_work)))
        json.dump(res_dic, f, indent=2)
        f.close()

class HarvestWorkInfo(luigi.Task):
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()

    def __init__(self, *args, **kwargs):
        super(HarvestWorkInfo, self).__init__(*args, **kwargs)
        filename = 'WorkInfo_%s.csv' % self.harvest_dts.strftime(luigi.DateMinuteParameter.date_format)
        self.dump_filepath = os.path.join(brd.config.REF_DATA_DIR, filename)

    def requires(self):
        return FetchWorkIdsWithoutInfo(self.n_work, self.harvest_dts)

    def output(self):
        from luigi import format
        return luigi.LocalTarget(self.dump_filepath, format=luigi.format.UTF8)

    def run(self):
        with self.input().open('r') as f:
            workids_list = json.load(f)
        spider_process = brd.scrapy.SpiderProcessor('workreference',
                                                    dump_filepath=self.dump_filepath,
                                                    works_to_harvest=workids_list)
        spider_process.start_process()
        logging.info("Harvest of %d work-info completed (dump file: '%s')"
                     % (len(workids_list), self.dump_filepath))

class BulkLoadWorkInfo(BaseBulkLoadTask):
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()

    input_has_headers = True
    table = 'staging.WORK_INFO'
    clear_table_before = True

    def requires(self):
        return HarvestWorkInfo(self.n_work, self.harvest_dts)

class LoadWorkNotFound(BasePostgresTask):
    """
    To load new work-id in integration.work.  This may happen with duplicate work-id
    redirecting to a master work-id not yet present in integration.work.
    These new id will only be linked to ISBN's on next load of thingISBN.
    """
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.WORK'

    def requires(self):
        return BulkLoadWorkInfo(self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work(refid, create_dts, load_audit_id)
            select work_refid, now(), %(audit_id)s
            from staging.work_info s
            left join integration.work w on (s.work_refid = w.refid)
            where w.refid IS NULL;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount

class LoadWorkInfo(BasePostgresTask):
    """
    Load work_info details. For duplicate, only "master" work is populated.
    """
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.WORK_INFO'

    def requires(self):
        return LoadWorkNotFound(self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
        """
        insert into integration.work_info(work_refid, title, ori_lang_code, mds_code, mds_text,
                                          lc_subjects, popularity, create_dts, load_audit_id)
        select s.work_refid, s.title, s.ori_lang_code, s.mds_code, s.mds_text,
               s.lc_subjects, s.popularity, now(), %(audit_id)s
        from staging.work_info s
        left join integration.work_info w on (w.work_refid = s.work_refid)
        where w.work_refid IS NULL;
        """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount

class LoadAuthor(BasePostgresTask):
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.AUTHOR'

    def requires(self):
        return BulkLoadWorkInfo(self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.author(id, code, create_dts, load_audit_id)
            select distinct cast(md5(s.code) as uuid), s.code, now(), %(audit_id)s
            from (  select unnest(string_to_array(authors_code,';')) as code
                    from staging.work_info
                 ) as s
            left join integration.author a on (s.code = a.code)
            where a.code IS NULL;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount

class LoadAuthorInfo(BasePostgresTask):
    """
    Do we need it at this point, or load, in separate task, more details at once
    """
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.AUTHOR_INFO'

    def requires(self):
        return LoadAuthor(self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        # group by author's code (same code may have diff name, ex. hxbrovivian)
        sql = \
            """
            insert into integration.author_info(author_id, create_dts, load_audit_id, name)
            select cast(md5(s.code) as uuid), now(), %(audit_id)s, max(s.name)
            from (  select unnest(string_to_array(authors_code,';')) as code,
                           unnest(string_to_array(authors,';')) as name
                    from staging.work_info
                 ) as s
            left join integration.author_info a on (cast(md5(s.code) as uuid) = a.author_id)
            where a.author_id IS NULL
            group by 1,2,3;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount

class LoadWorkAuthor(BasePostgresTask):
    """
    Load relationship work-author
    """
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.WORK_AUTHOR'

    def requires(self):
        return [LoadAuthorInfo(self.n_work, self.harvest_dts),
                LoadWorkNotFound(self.n_work, self.harvest_dts)]

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work_author(author_id, work_refid, create_dts, load_audit_id)
            select distinct cast(md5(s.code) as uuid), s.work_refid, now(), %(audit_id)s
            from (  select unnest(string_to_array(authors_code,';')) as code,
                           work_refid
                    from staging.work_info
                 ) as s
            left join integration.work_author wa on
              (cast(md5(s.code) as uuid) = wa.author_id and s.work_refid = wa.work_refid)
            where wa.author_id IS NULL and wa.work_refid IS NULL;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount

class LoadWorkSameAs(BasePostgresTask):
    """
    Flag duplicate work-id.
    """
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.WORK_SAMEAS'

    def requires(self):
        return LoadWorkNotFound(self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work_sameas(work_refid, master_refid, create_dts, load_audit_id)
            select distinct dup_refid, work_refid, now(), %(audit_id)s
            from staging.work_info
            where dup_refid is not null;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount


# python -m luigi --module brd.task BatchLoadWorkInfo --n-work 2 --harvest-dts 2016-01-31T1930 --local-scheduler
class BatchLoadWorkInfo(luigi.Task):
    """
    Entry point to launch work-info harvest loads
    """
    n_work = luigi.Parameter(default=100)
    harvest_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())

    global batch_name
    batch_name = "Load Work-info"

    def requires(self):
        return [LoadWorkInfo(self.n_work, self.harvest_dts), LoadWorkAuthor(self.n_work, self.harvest_dts),
                LoadWorkSameAs(self.n_work, self.harvest_dts)]

