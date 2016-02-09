
import luigi
import luigi.postgres
import json
import brd
import brd.config
import brd.service
import datetime
import os
import brd.scrapy
import luigi.parameter
from luigi import six

# global batch_name by Batch entry point Task (shared among all task)
batch_name = "n.a."  # ok, as concurrent batch jobs are launched in separate process


class DateSecondParameter(luigi.parameter.DateHourParameter):
    """
    My version of Parameter whose value is a :py:class:`~datetime.datetime` specified
    to the second (luigi only provides DateParam up to the minute)

    A DateSecondParameter is formatted date and time specified to the second.
    For example, ``2013-07-10T190723`` specifies July 10, 2013 at 19:07:23.
    """

    date_format = '%Y-%m-%dT%H%M%S'
    _timedelta = luigi.parameter.timedelta(seconds=1)

    def parse(self, s):
        return super(DateSecondParameter, self).parse(s)


def postgres_target(target_table, update_id):
    return luigi.postgres.PostgresTarget(
            host        =brd.config.DATABASE['host'],
            database    =brd.config.DATABASE['database'],
            user        =brd.config.DATABASE['user'],
            password    =brd.config.DATABASE['password'],
            table       =target_table,
            update_id   =update_id)


class MyBasePostgresTask(luigi.Task):
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

        # decorate with audit-log stuff
        self.audit_id, self.run_dts = brd.elt.insert_auditing(batch_name, self.task_id)
        self.rowscount = self.exec_sql(connection, self.audit_id)

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

    def exec_sql(self, connection, audit_id):
        raise NotImplementedError




class MyBaseBulkLoadTask(luigi.postgres.CopyToTable):
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

    # default separator
    column_separator = '|'

    # added to manage col headers
    input_has_headers = False

    def requires(self):
        raise NotImplementedError

    def run(self):
        if self.input_has_headers:
            with self.input().open('r') as fobj:
                header = fobj.next()
            self.columns = header.strip('\n').split(self.column_separator)

        # decorate with audit-log stuff
        self.audit_id, self.run_dts = brd.elt.insert_auditing(batch_name, self.task_id)
        super(MyBaseBulkLoadTask, self).run()


    # the override copy() is a hack to manage file with col headers and rowscount
    # TODO: suggest to add to luigi to accept headers and populate columns based on these..
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
        sql = \
            """
            copy %s( %s )
            from STDIN with csv HEADER DELIMITER '%s' NULL '\\N';
            """ % (self.table, ",".join(self.columns), self.column_separator)

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


# ------------------- real Task implementation section ...------------------------ #

class DownLoadThingISBN(luigi.Task):
    filepath = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filepath)


class BulkLoadThingISBN(MyBaseBulkLoadTask):
    """
    This task bulkload thingisbn file into staging
    TODO: truncate staging table first not to accumulate older reference
    """
    thingisbn_filename = luigi.Parameter()
    table = 'staging.THINGISBN'
    columns = ["WORK_UID", "ISBN_ORI", "ISBN10", "ISBN13", "LOAD_AUDIT_ID"]

    def requires(self):
        fullpath_name = os.path.join(brd.config.REF_DATA_DIR, self.thingisbn_filename)
        return DownLoadThingISBN(fullpath_name)


class LoadWorkRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    table = 'integration.work'

    def requires(self):
        return BulkLoadThingISBN(self.thingisbn_filename)

    def exec_sql(self, connection, audit_id):
        sql = \
            """
            insert into integration.work(uid, create_dts, load_audit_id)
            select distinct s.work_uid, now(), %(audit_id)s
            from staging.thingisbn s
            left join integration.work w on s.work_uid = w.uid
            where w.uid is null;
            """
        return brd.elt.execute_and_get_rowcount(connection, sql, {'audit_id': audit_id})


class LoadIsbnRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    table = 'integration.isbn'

    def requires(self):
        return BulkLoadThingISBN(self.thingisbn_filename)

    def exec_sql(self, connection, audit_id):
        sql = \
            """
            insert into integration.isbn(ean, isbn13, isbn10, create_dts, load_audit_id)
            select distinct cast(s.isbn13 as bigint), s.isbn13, s.isbn10, now(), %(audit_id)s
            from staging.thingisbn s
            left join integration.isbn i on cast(s.isbn13 as bigint) = i.ean
            where i.ean is null;
            """
        return brd.elt.execute_and_get_rowcount(connection, sql, {'audit_id': audit_id})


class LoadWorkIsbnRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    table = 'integration.work_isbn'

    def requires(self):
        return [LoadWorkRef(self.thingisbn_filename), LoadIsbnRef(self.thingisbn_filename)]

    def exec_sql(self, connection, audit_id):
        sql = \
            """
            insert into integration.work_isbn(ean, work_uid, source_site_id, create_dts, load_audit_id)
            select distinct cast(s.isbn13 as bigint), s.work_uid, (select id from integration.site where logical_name = 'librarything')
                    , now(), %(audit_id)s
            from staging.thingisbn s
            left join integration.work_isbn i on (cast(s.isbn13 as bigint) = i.ean and s.work_uid = i.work_uid)
            where i.ean is null;
            """
        return brd.elt.execute_and_get_rowcount(connection, sql, {'audit_id': audit_id})


class LoadWorkSiteMappingRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    site_logical_name = luigi.Parameter(default='librarything')

    table = 'integration.work_site_mapping'

    def requires(self):
        return [LoadWorkRef(self.thingisbn_filename)]

    def exec_sql(self, connection, audit_id):
        sql = \
            """
            insert into integration.work_site_mapping(ref_uid, work_id, site_id, work_ori_id, create_dts, load_audit_id)
            select work.uid, integration.derive_other_work_id(work.uid::text,%(logical_name)s)
                    , work.site_id, work.uid_text, now(), %(audit_id)s
            from
            (select uid, uid::text as uid_text, (select id from integration.site where logical_name = %(logical_name)s) as site_id
            from integration.work) as work
            left join integration.work_site_mapping m on (work.uid_text = m.work_ori_id and m.site_id = work.site_id)
            where m.work_ori_id IS NULL;
            """
        return brd.elt.execute_and_get_rowcount(connection, sql, {'logical_name': self.site_logical_name,
                                                                  'audit_id': audit_id})


class BatchLoadWorkReference(luigi.Task):
    """
    Entry point to launch all loads related to 'thingisbn.csv' filen
    """
    thingisbn_filename = luigi.Parameter(default='thingISBN_10000_26-1-2016.csv')

    global batch_name
    batch_name = "Batch: reference thingisbn"  # for auditing

    def requires(self):
        return [LoadWorkIsbnRef(self.thingisbn_filename), LoadWorkSiteMappingRef(self.thingisbn_filename)]


class FetchWorkids(luigi.Task):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()

    def output(self):
        wids_filepath = '/tmp/wids_%s_%s.txt' % (self.spidername,
                                                 self.harvest_dts.strftime(DateSecondParameter.date_format))
        return luigi.LocalTarget(wids_filepath)

    def run(self):
        f = self.output().open('w')
        for wid in brd.service.fetch_work(self.spidername, harvested=False, nb_work=self.n_work):
            json.dump(wid, f)
            f.write('\n')
        f.close()


class HarvestReviews(luigi.Task):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()

    def __init__(self, *args, **kwargs):
        super(HarvestReviews, self).__init__(*args, **kwargs)

        filename = 'ReviewsOf%s_%s.csv' % (self.spidername,
                                           self.harvest_dts.strftime(DateSecondParameter.date_format))
        self.dump_filepath = os.path.join(brd.config.SCRAPED_OUTPUT_DIR, filename)

    def requires(self):
        return FetchWorkids(self.spidername, self.n_work, self.harvest_dts)

    def output(self):
        from luigi import format
        return luigi.LocalTarget(self.dump_filepath, format=luigi.format.UTF8)

    def run(self):
        with self.input().open('r') as f:
            workids_list = parse_wids_file(f)

        spider_process = brd.scrapy.SpiderProcessor(self.spidername,
                                                    dump_filepath=self.dump_filepath,
                                                    works_to_harvest=workids_list)
        spider_process.start_process()

        print("Harvest of %d works/review completed with spider %s (dump file: '%s')"
              % (len(workids_list), self.spidername, self.dump_filepath))


class UpdateLastHarvestDate(MyBasePostgresTask):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()
    table = 'integration.WORK_SITE_MAPPING'

    def requires(self):
        return {'wids': FetchWorkids(self.spidername, self.n_work, self.harvest_dts),
                'harvest': HarvestReviews(self.spidername, self.n_work, self.harvest_dts)}

    def exec_sql(self, connection, audit_id):
        with self.input()['wids'].open('r') as f:
            wids = tuple(parse_wids_file(f, True))

        sql = \
            """
            update integration.work_site_mapping
            set last_harvest_dts = %(end)s
            where
            site_id = (select id from integration.site where logical_name = %(name)s)
            and work_ori_id IN %(w_ids)s;
            """
        return brd.elt.execute_and_get_rowcount(connection,
                                                sql, {'end': self.harvest_dts,
                                                      'name': self.spidername,
                                                      'w_ids': wids})


class BulkLoadReviews(MyBaseBulkLoadTask):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()

    input_has_headers = True
    table = 'staging.REVIEW'
    # use my hack instead to guarantee the right column order
    # columns = ["username","rating","user_uid","parsed_review_date","review_date","work_uid","review","parsed_rating","site_logical_name","likes"]

    def requires(self):
        return HarvestReviews(self.spidername, self.n_work, self.harvest_dts)


class LoadReviews(MyBasePostgresTask):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()
    table = 'integration.REVIEW'

    def requires(self):
        return BulkLoadReviews(self.spidername, self.n_work, self.harvest_dts)

    def exec_sql(self, connection, audit_id):
        sql = \
            """
            insert into integration.review
             (work_uid, user_id, site_id, lang_code, rating, review, review_date, create_dts, load_audit_id)
                select
                    integration.derive_bookid( integration.get_sform(r.book_title),
                        r.book_lang,
                        integration.standardize_authorname(r.author_fname, r.author_lname) ) as bookid
                        , integration.derive_reviewerid( r.username, r.site_logical_name ) as reviewerid
                        , r.parsed_review_date
                        , r.review_rating
                        , now()
                        , %(audit_id)s
                from staging.review r
            """
        return brd.elt.execute_and_get_rowcount(connection, sql,
                                                {'logical_name': self.site_logical_name,
                                                 'audit_id': audit_id})



class LoadWorkSameAs(MyBasePostgresTask):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()
    table = 'integration.WORK_SAMEAS'








# python -m luigi --module brd.task BatchLoadReviews --spidername librarything --n-work 2 --harvest-dts 2016-01-31T190723 --local-scheduler
class BatchLoadReviews(luigi.Task):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    # for debug use, default should not be used otherwise different task will be launched even after failure
    harvest_dts = DateSecondParameter(default=datetime.datetime.now())

    global batch_name
    batch_name = "Batch: integrate reviews"  # for auditing

    def requires(self):
        return [HarvestReviews(self.spidername, self.n_work, self.harvest_dts),
                UpdateLastHarvestDate(self.spidername, self.n_work, self.harvest_dts)]


def parse_wids_file(wf, only_ids=False):
    list_wids = []
    for line in wf:
        jw = json.loads(line)
        if only_ids:
            list_wids.append(jw['work-ori-id'])
        else:
            list_wids.append(jw)
    return list_wids

