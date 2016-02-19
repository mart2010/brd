
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


class DateSecParameter(luigi.parameter.DateHourParameter):
    """
    My version of Parameter whose value is a :py:class:`~datetime.datetime` specified
    to the second (luigi only provides DateParam up to the minute)

    A DateSecondParameter is formatted date and time specified to the second.
    For example, ``2013-07-10T190723`` specifies July 10, 2013 at 19:07:23.
    """

    date_format = '%Y-%m-%dT%H%M%S'
    _timedelta = luigi.parameter.timedelta(seconds=1)

    def parse(self, s):
        return super(DateSecParameter, self).parse(s)


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

    # container of values inserted as NULL values (MO added empty string)
    null_values = (None, "")

    # added to manage col headers
    input_has_headers = False

    def requires(self):
        raise NotImplementedError

    def __init__(self, *args, **kwargs):
        super(MyBaseBulkLoadTask, self).__init__(*args, **kwargs)
        self.run_dts = None
        self.audit_id = None
        self.rowscount = None

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
        # NULL '%s':now leave default NULL (empty and \N as luigi generated tmp file does not produce the \\N correctly)
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
    filename = luigi.Parameter()
    table = 'staging.THINGISBN'
    columns = ["WORK_REFID", "ISBN_ORI", "ISBN10", "ISBN13"]

    def requires(self):
        fullpath_name = os.path.join(brd.config.REF_DATA_DIR, self.filename)
        return DownLoadThingISBN(fullpath_name)

    def init_copy(self, connection):
        # clear staging thingisbn before!
        connection.cursor().execute('truncate table %s;' % self.table)


class LoadWorkRef(MyBasePostgresTask):
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


class LoadIsbnRef(MyBasePostgresTask):
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
class LoadWorkIsbnRef(MyBasePostgresTask):
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
    filename = luigi.Parameter(default='thingISBN_10000_26-1-2016.csv')

    global batch_name
    batch_name = "Ref thingisbn"  # for auditing

    def requires(self):
        return [LoadWorkIsbnRef(self.filename)]


class FetchNewWorkIds(luigi.Task):
    """
    This fetches n_work NOT yet harvested for site and their associated isbns
    Used before harvesting sites where the work-uid is unknowns.
    """
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecParameter()

    def output(self):
        wids_filepath = '/tmp/newwids_%s_%s.txt' % \
                        (self.site, self.harvest_dts.strftime(DateSecParameter.date_format))
        return luigi.LocalTarget(wids_filepath)

    def run(self):
        f = self.output().open('w')
        res_dic = brd.service.fetch_workwithisbn_not_harvested(self.site, nb_work=self.n_work)
        json.dump(res_dic, f, indent=2)
        f.close()


class HarvestReviews(luigi.Task):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecParameter()

    def __init__(self, *args, **kwargs):
        super(HarvestReviews, self).__init__(*args, **kwargs)

        filename = 'ReviewsOf%s_%s.csv' % (self.site,
                                           self.harvest_dts.strftime(DateSecParameter.date_format))
        self.dump_filepath = os.path.join(brd.config.SCRAPED_OUTPUT_DIR, filename)

    def requires(self):
        return FetchNewWorkIds(self.site, self.n_work, self.harvest_dts)

    def output(self):
        from luigi import format
        return luigi.LocalTarget(self.dump_filepath, format=luigi.format.UTF8)

    def run(self):
        with self.input().open('r') as f:
            workids_list = json.load(f)
        spider_process = brd.scrapy.SpiderProcessor(self.site,
                                                    dump_filepath=self.dump_filepath,
                                                    works_to_harvest=workids_list)
        spider_process.start_process()

        print("Harvest of %d works/review completed with spider %s (dump file: '%s')"
              % (len(workids_list), self.site, self.dump_filepath))


class BulkLoadReviews(MyBaseBulkLoadTask):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecParameter()

    input_has_headers = True
    table = 'staging.REVIEW'

    def requires(self):
        return HarvestReviews(self.site, self.n_work, self.harvest_dts)


class LoadUsers(MyBasePostgresTask):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecParameter()

    table = 'integration.USER'

    def requires(self):
        return BulkLoadReviews(self.site, self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            with new_rows as (
                select distinct integration.derive_userid(user_uid, %(site)s) as id
                    , user_uid
                    , (select id from integration.site where logical_name = %(site)s) as site_id
                    , username
                    , now() as last_seen_date
                    , now() as create_dts
                    , %(audit_id)s as audit_id
                from staging.review
                where site_logical_name = %(site)s
            ),
            match_user as (
                update integration.user u set last_seen_date = new_rows.last_seen_date
                from new_rows
                where u.id = new_rows.id
                returning u.*
            )
            insert into integration.user(id, user_uid, site_id, username, last_seen_date, create_dts, load_audit_id)
            select id, user_uid, site_id, username, last_seen_date, create_dts, audit_id
            from new_rows
            where not exists (select 1 from match_user where match_user.id = new_rows.id);
            """
        cursor.execute(sql, {'audit_id': audit_id, 'site': self.site})
        r = cursor.rowcount
        print "check this should return two counts .. update and insert: " + str(r)
        return r


class LoadReviews(MyBasePostgresTask):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecParameter()
    table = 'integration.REVIEW'

    def requires(self):
        return LoadUsers(self.site, self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.review
                (work_refid, user_id, site_id, rating, review, review_date, review_lang, create_dts, load_audit_id)
                select  r.work_refid
                    , integration.derive_userid(r.user_uid, %(site)s) as user_id
                    , s.id as site_id
                    , r.rating
                    , r.review
                    , r.parsed_review_date
                    , r.review_lang
                    , now()
                    , %(audit_id)s
                from staging.review r
                join integration.site s on (r.site_logical_name = s.logical_name and s.logical_name = %(site)s)
                where r.user_uid is not null
            """
        cursor.execute(sql, {'audit_id': audit_id, 'site': self.site})
        return cursor.rowcount


class LoadWorkSameAs(MyBasePostgresTask):
    """
    Used only when loading reviews from lt, to link same works (duplicates)
    """
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecParameter()
    table = 'integration.WORK_SAMEAS'

    def requires(self):
        return BulkLoadReviews(self.site, self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work_sameas(work_refid, master_refid, create_dts, load_audit_id)
            select distinct cast(dup_refid as bigint), cast(work_refid as bigint), now(), %(audit_id)s
            from staging.review r
            where site_logical_name = 'librarything'
            and dup_refid is not null;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount


class UpdateLtLastHarvest(MyBasePostgresTask):
    """
    Updates both work_refid and associated master work_refid (if any), so
    make sure update work_sameas was processed!
    (note: for other site, this gets done in task LoadWorkSiteMapping)
    """
    n_work = luigi.IntParameter()
    harvest_dts = DateSecParameter()
    table = 'integration.WORK'

    def requires(self):
        return {FetchNewWorkIds('librarything', self.n_work, self.harvest_dts),
                LoadWorkSameAs('librarything', self.n_work, self.harvest_dts)}

    def exec_sql(self, cursor, audit_id):
        with self.input().open('r') as f:
            res = json.load(f)
        wids = tuple(row['work_refid'] for row in res)
        sql = \
            """
            with all_refid as (
                select refid
                from integration.work
                where refid IN %(w_ids)s
                union
                select master_refid
                from integration.work_sameas
                where work_refid IN %(w_ids)s
                and master_refid IS NOT NULL
            )
            update integration.work w
            set last_harvest_dts = %(end)s
            where w.refid in (select refid from all_refid);
            """
        cursor.execute(sql, {'end': self.harvest_dts, 'w_ids': wids})
        return cursor.rowcount


# python -m luigi --module brd.task BatchLoadReviews --site librarything
# --n-work 2 --harvest-dts 2016-01-31T190723 --local-scheduler
class BatchLoadReviews(luigi.Task):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    # for debug use, default should not be used otherwise different task will be launched even after failure
    harvest_dts = DateSecParameter(default=datetime.datetime.now())

    global batch_name
    batch_name = "Reviews"  # for auditing

    def requires(self):
        requirements = [LoadReviews(self.site, self.n_work, self.harvest_dts),
                        UpdateOtherLastHarvestDate(self.site, self.n_work, self.harvest_dts)]
        if self.site == 'librarything':
            requirements.append(LoadReviews(self.site, self.n_work, self.harvest_dts),
                                UpdateLtLastHarvest(self.n_work, self.harvest_dts))
        return requirements


class LoadWorkSiteMapping(MyBasePostgresTask):
    """
    When mapping is done after review harvest (by searching isbn)
    """
    filename = luigi.Parameter()
    site = luigi.Parameter()
    harvest_dts = DateSecParameter()

    table = 'integration.WORK_SITE_MAPPING'

    def requires(self):
        return [LoadWorkRef(self.filename)]

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work_site_mapping(work_refid, work_uid, site_id, last_harvest_dts, create_dts, load_audit_id)
            select work_refid, work_uid, (select id from integration.site where logical_name = %(logical_name)s),
            %(harvest_dts)s, now(), %(audit_id)s
            from
            staging.review
            where site_logical_name = %(logical_name)s
            """
        cursor.execute(sql, {'audit_id': audit_id,
                             'logical_name': self.site,
                             'harvest_dts': self.harvest_dts})
        return cursor.rowcount


class UpdateOtherLastHarvestDate(MyBasePostgresTask):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecParameter()
    table = 'integration.WORK_SITE_MAPPING'

    def requires(self):
        return {FetchNewWorkIds(self.site, self.n_work, self.harvest_dts)}

    def exec_sql(self, cursor, audit_id):
        with self.input().open('r') as f:
            wids = tuple(parse_wids_file(f, True))
        sql = \
            """
            update integration.work_site_mapping
            set last_harvest_dts = %(end)s
            where
            site_id = (select id from integration.site where logical_name = %(name)s)
            and work_ori_id IN %(w_ids)s;
            """
        cursor.execute(sql, {'end': self.harvest_dts, 'name': self.site, 'w_ids': wids})
        return cursor.rowcount

