import json
import logging

import datetime

import brd
import brd.config
import brd.scrapy
import brd.elt as elt
import brd.service as service
import luigi
import luigi.postgres
import os
from brd.taskbase import BasePostgresTask, BaseBulkLoadTask, batch_name
__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The BRD Project"

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------------- #
# ----------------------------------  LOAD THINGISBN ------------------------------------------ #
# file 'thingISBN.xml.gz' located at http://www.librarything.com/feeds/
# --------------------------------------------------------------------------------------------- #

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
            with stage as (
                select distinct work_refid
                from staging.thingisbn
            ), matching as (
                update integration.work w set last_seen_date = now()
                from stage
                where stage.work_refid = w.refid
                returning w.*
            )
            insert into integration.work(refid, last_seen_date, create_dts, load_audit_id)
            select work_refid, now(), now(), %(audit_id)s
            from stage
            where not exists (select 1 from matching where matching.refid = stage.work_refid)
            ;
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
            with stage as (
                select distinct cast(isbn13 as bigint) as ean, isbn13, isbn10
                from staging.thingisbn
            ), match as (
                update integration.isbn i set last_seen_date = now()
                from stage
                where stage.ean = i.ean
                returning i.*
            )
            insert into integration.isbn(ean, isbn13, isbn10, last_seen_date, create_dts, load_audit_id)
            select ean, isbn13, isbn10, now(), now(), %(audit_id)s
            from stage
            where not exists (select 1 from match where match.ean = stage.ean)
            ;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount



# Thingisbn has a few data issues:
# - full duplicate in work-id, isbn
# - some isbn/ean are associated with more than 1 work!
# These associations cannot be loaded.
#brd=> select cnt, count(1) from (select ean, count(1) as cnt from work_isbn group by 1) as foo group by 1;
# cnt |  count
#-----+---------
#   4 |       2
#   1 | 6243415
#   3 |    2220
#   2 |  126306
#(4 rows)

class LoadWorkIsbnRef(BasePostgresTask):
    """
    Load/update work_isbn from a full and cleansed thingISBN batch
    NOT meant for incremental/subset load of isbn/work.
    Cleansing on thingISBN removes full duplicates and all isbn associated to more than one work.
    Steps done:
    1) Update last_seen_date/deletion_date on existing (isbn,work) couple
    2) Insert new (isbn,work) couple
    3) Update deletion_date on NO LONGER existing (isbn,work)
    """
    filename = luigi.Parameter()
    table = 'integration.work_isbn'

    def requires(self):
        return [LoadWorkRef(self.filename), LoadIsbnRef(self.filename)]

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            with stage as (
                select distinct work_refid, cast(t.isbn13 as bigint) as ean, t.isbn13, isbn10
                from staging.thingisbn t
                left join
                    (select isbn13, count(1)
                    from staging.thingisbn
                    group by 1
                    having count(1) > 1) as error_isbn on (t.isbn13 = error_isbn.isbn13)
                where error_isbn.isbn13 IS NULL),
            nonmatch as (
                update integration.work_isbn w set deletion_date = now()
                where not exists (select 1 from stage where stage.work_refid = w.work_refid and stage.ean = w.ean)),
            match as (
                update integration.work_isbn wi set last_seen_date = now(), deletion_date = NULL
                from stage
                where stage.work_refid = wi.work_refid and stage.ean = wi.ean
                returning wi.*)
            insert into integration.work_isbn(work_refid, ean, last_seen_date, create_dts, load_audit_id)
            select work_refid, ean, now(), now(), %(audit_id)s
            from stage
            where not exists (select 1 from match
                              where match.work_refid = stage.work_refid
                              and match.ean = stage.ean)
            ;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount

# python -m luigi --module brd.taskref BatchLoadWorkRef --filename thingISBN_2016_1804.csv --local-scheduler
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
    work-ids (to avoid re-harvesting same work through its duplicate)
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
        nb_av = len(res_dic)
        if nb_av == 0:
            raise brd.WorkflowError("No more work without info available, STOP PROCESSING!")
        elif nb_av < int(self.n_work):
            logger.info("Only %d ids found without info (out of %s requested)" % (nb_av, self.n_work))
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
        logger.info("Harvest of %d work-info completed (dump file: '%s')"
                     % (len(workids_list), self.dump_filepath))

class BulkLoadWorkInfo(BaseBulkLoadTask):
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()

    input_has_headers = True
    table = 'staging.WORK_INFO'
    clear_table_before = True

    def requires(self):
        return HarvestWorkInfo(self.n_work, self.harvest_dts)

class LoadWorkMissing(BasePostgresTask):
    """
    To load new work-id in integration.work.  This may happen with duplicate work-id
    redirecting to a master work-id missing from integration.work.
    These new id will be linked to ISBN's when updating thingISBN.
    """
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.WORK'

    def requires(self):
        return BulkLoadWorkInfo(self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work(refid, create_dts, last_seen_date, load_audit_id)
            select work_refid, now(), now(), %(audit_id)s
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
        return LoadWorkMissing(self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work_info(work_refid, title, original_lang, ori_lang_code, mds_code, mds_code_ori,
                                              lc_subjects, popularity, create_dts, load_audit_id)
            select s.work_refid, s.title, s.original_lang, s.ori_lang_code, replace(coalesce(s.mds_code_corr, s.mds_code),'.',''),
                   s.mds_code, s.lc_subjects, cast(replace(s.popularity,',','') as int), now(), %(audit_id)s
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
                LoadWorkMissing(self.n_work, self.harvest_dts)]

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
        return LoadWorkMissing(self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work_sameas(work_refid, master_refid, create_dts, load_audit_id)
            select distinct wi.dup_refid, wi.work_refid, now(), %(audit_id)s
            from staging.work_info wi
            left join integration.work_sameas ws on (wi.dup_refid = ws.work_refid and wi.work_refid = ws.master_refid)
            where wi.dup_refid IS NOT NULL
            and ws.work_refid IS NULL
            ;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount

# Almost 50% of LC subjects are NULL!!! not integrated for now
class LoadLcSubjects(BaseBulkLoadTask):
    pass

# A bit more than 10% of MDS are NULL
class LoadMDS(BasePostgresTask):
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.MDS'

    def requires(self):
        return BulkLoadWorkInfo(self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        # some text are different for same code corrected (keep arbitrarily the max)
        sql = \
            """
            insert into integration.mds(code, code_w_dot, mds_text, create_dts, load_audit_id)
            select  replace(coalesce(mds_code_corr,mds_code),'.',''),
                    coalesce(mds_code_corr,mds_code),
                    max(mds_text),
                    now(),
                    %(audit_id)s
            from staging.work_info source
            where
            coalesce(mds_code_corr,mds_code) is not null
            and not exists (select code from integration.mds t
                            where t.code = replace(coalesce(source.mds_code_corr,source.mds_code),'.','')
                           )
            group by 1,2;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount

class LoadWorkLangTitle(BasePostgresTask):
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.work_title'

    def requires(self):
        return BulkLoadWorkInfo(self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        # Add language text not present in from language reference
        # filter out empty language string in web page
        #  lang_title[2:5] used to avoid splitting WHEN title has ':' (4 of these are possible ;-!)
        sql = \
            """
            with title_harvest as
            (select work_refid, trim(lang_title[1]) lang, trim(array_to_string(lang_title[2:5],'')) as title
             from (
                  select work_refid,
                     string_to_array(unnest(string_to_array(other_lang_title,'__&__')),':') as lang_title
                  from staging.work_info) as foo
            )
            insert into integration.work_title(work_refid, lang_code, lang_text, title, create_dts, load_audit_id)
            select t.work_refid, l.code, t.lang, t.title, now(), %(audit_id)s
            from title_harvest t
            left join integration.language l on (upper(l.english_name) = upper(t.lang))
            where
            char_length(t.lang::text) > 1
            and not exists (select 1 from integration.work_title existing
                            where existing.work_refid = t.work_refid and existing.lang_text = t.lang);
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount

# python -m luigi --module brd.taskref BatchLoadWorkInfo --n-work 100000 --local-scheduler
class BatchLoadWorkInfo(luigi.Task):
    """
    Entry point to launch work-info harvest loads
    """
    n_work = luigi.Parameter(default=100)
    harvest_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())

    global batch_name
    batch_name = "Load Work-info"

    def requires(self):
        return [LoadWorkInfo(self.n_work, self.harvest_dts),
                LoadWorkAuthor(self.n_work, self.harvest_dts),
                LoadWorkSameAs(self.n_work, self.harvest_dts),
                LoadWorkLangTitle(self.n_work, self.harvest_dts),
                LoadMDS(self.n_work, self.harvest_dts)]


# --------------------------------------------------------------------------------------------- #
# -------------------------------------  ISBN LANG  ------------------------------------------- #
# request:  http://www.librarything.com/api/thingLang.php?isbn=
# --------------------------------------------------------------------------------------------- #

class FetchIsbnList(luigi.Task):
    n_isbn = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())

    def output(self):
        filepath = '/tmp/isbns_lang_%s.txt' % self.harvest_dts.strftime(luigi.DateMinuteParameter.date_format)
        return luigi.LocalTarget(filepath)

    def run(self):
        # order by work_refid to align with review harvesting execution
        sql = \
            """
            select w.ean::text
            from integration.work_isbn w
            left join integration.isbn_info i on (w.ean = i.ean)
            where i.lang_code IS NULL
            order by w.work_refid
            limit %(nb)s
            """
        conn = elt.get_ro_connection()
        tup_list = conn.fetch_all(sql, params={'nb': self.n_isbn})
        f = self.output().open('w')
        f.write(";".join([tup[0] for tup in tup_list]))
        f.close()

class HarvestIsbnLang(luigi.Task):
    n_isbn = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())

    def __init__(self, *args, **kwargs):
        super(HarvestIsbnLang, self).__init__(*args, **kwargs)
        filename = 'ISBN_Lang_%s.csv' % self.harvest_dts.strftime(luigi.DateMinuteParameter.date_format)
        self.dump_filepath = os.path.join(brd.config.REF_DATA_DIR, filename)

    def requires(self):
        return FetchIsbnList(self.n_isbn, self.harvest_dts)

    def output(self):
        return luigi.LocalTarget(self.dump_filepath)

    def run(self):
        with self.input().open('r') as f:
            ean_list = f.readline().rstrip('\n').split(';')

        spider_process = brd.scrapy.SpiderProcessor('isbnlanguage',
                                                    dump_filepath=self.dump_filepath,
                                                    isbns_to_fetch=ean_list)
        spider_process.start_process()
        logger.info("Harvest of %d isbn-lang completed (dump file: '%s')"
                    % (len(ean_list), self.dump_filepath))


class BulkLoadIsbnLang(BaseBulkLoadTask):
    n_isbn = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())

    input_has_headers = True
    table = 'staging.ISBN_LANG'
    clear_table_before = True

    def requires(self):
        return HarvestIsbnLang(self.n_isbn, self.harvest_dts)


class LoadIsbnLang(BasePostgresTask):
    """
    Load isbn_info with language only.
    """
    n_isbn = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())
    table = 'integration.ISBN_INFO'

    def requires(self):
        return BulkLoadIsbnLang(self.n_isbn, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            with stage as (
                select distinct ean::bigint as ean, lang_code
                from staging.isbn_lang
            ), match as (
                update integration.isbn_info i set lang_code = stage.lang_code, update_dts = now()
                from stage
                where i.ean = stage.ean
                returning i.*
            )
            insert into integration.isbn_info(ean, lang_code, create_dts, load_audit_id)
            select s.ean, s.lang_code, now(), %(audit_id)s
            from stage s
            left join match on (s.ean = match.ean)
            where match.ean IS NULL
            ;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount


# python -m luigi --module brd.taskref BatchLoadIsbnLang --n-isbn 50000 --local-scheduler
class BatchLoadIsbnLang(luigi.Task):
    """
    Entry point to launch isbn-lang harvest loads
    """
    n_isbn = luigi.Parameter(default=100)
    harvest_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())

    global batch_name
    batch_name = "Load Isbn-lang"

    def requires(self):
        return [LoadIsbnLang(self.n_isbn, self.harvest_dts)]
