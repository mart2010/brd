
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
    thingisbn_filename = luigi.Parameter()
    table = 'staging.THINGISBN'
    columns = ["WORK_UID", "ISBN_ORI", "ISBN10", "ISBN13"]

    def requires(self):
        fullpath_name = os.path.join(brd.config.REF_DATA_DIR, self.thingisbn_filename)
        return DownLoadThingISBN(fullpath_name)

    def init_copy(self, connection):
        # clear staging thingisbn before!
        connection.cursor().execute('truncate table %s;' % self.table)


class LoadWorkRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    table = 'integration.work'

    def requires(self):
        return BulkLoadThingISBN(self.thingisbn_filename)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work(uid, create_dts, load_audit_id)
            select distinct s.work_uid, now(), %(audit_id)s
            from staging.thingisbn s
            left join integration.work w on s.work_uid = w.uid
            where w.uid is null;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount


class LoadIsbnRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    table = 'integration.isbn'

    def requires(self):
        return BulkLoadThingISBN(self.thingisbn_filename)

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

# TODO: what about possible deletion of work_uid/isbn (relevant also for work and isbn table)
class LoadWorkIsbnRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    table = 'integration.work_isbn'

    def requires(self):
        return [LoadWorkRef(self.thingisbn_filename), LoadIsbnRef(self.thingisbn_filename)]

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work_isbn(ean, work_uid, source_site_id, create_dts, load_audit_id)
            select distinct cast(s.isbn13 as bigint), s.work_uid, (select id from integration.site where logical_name = 'librarything')
                    , now(), %(audit_id)s
            from staging.thingisbn s
            left join integration.work_isbn i on (cast(s.isbn13 as bigint) = i.ean and s.work_uid = i.work_uid)
            where i.ean is null;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount


class BatchLoadWorkRef(luigi.Task):
    """
    Entry point to launch loads related to 'thingisbn.csv' reference file
    """
    thingisbn_filename = luigi.Parameter(default='thingISBN.csv')

    global batch_name
    batch_name = "Ref thingisbn"  # for auditing

    def requires(self):
        return [LoadWorkIsbnRef(self.thingisbn_filename)]


class FetchLtWorkids(luigi.Task):
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()

    def output(self):
        wids_filepath = '/tmp/ltwids_%s.txt' % (self.harvest_dts.strftime(DateSecondParameter.date_format))
        return luigi.LocalTarget(wids_filepath)

    def run(self):
        f = self.output().open('w')
        for wid in brd.service.fetch_work_list(self.spidername, harvested=False, nb_work=self.n_work):
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
        return FetchLtWorkids(self.spidername, self.n_work, self.harvest_dts)

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


class BulkLoadReviews(MyBaseBulkLoadTask):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()

    input_has_headers = True
    table = 'staging.REVIEW'

    def requires(self):
        return HarvestReviews(self.spidername, self.n_work, self.harvest_dts)


class LoadUsers(MyBasePostgresTask):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()

    table = 'integration.USER'

    def requires(self):
        return BulkLoadReviews(self.spidername, self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            with new_rows as (
                select distinct integration.derive_userid(user_uid, %(spidername)s) as id
                    , user_uid
                    , (select id from integration.site where logical_name = %(spidername)s) as site_id
                    , username
                    , now() as last_seen_date
                    , now() as create_dts
                    , %(audit_id)s as audit_id
                from staging.review
                where site_logical_name = %(spidername)s
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
        cursor.execute(sql, {'audit_id': audit_id, 'spidername': self.spidername})
        r = cursor.rowcount
        print "check this should return two counts .. update and insert: " + str(r)
        return r


class LoadReviews(MyBasePostgresTask):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()
    table = 'integration.REVIEW'

    def requires(self):
        return LoadUsers(self.spidername, self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.review
                (work_uid, user_id, site_id, rating, review, review_date, review_lang, create_dts, load_audit_id)
                select  m.ref_uid
                    , integration.derive_userid(r.user_uid, %(spidername)s) as user_id
                    , s.id as site_id
                    , r.rating
                    , r.review
                    , r.parsed_review_date
                    , r.review_lang
                    , now()
                    , %(audit_id)s
                from staging.review r
                join integration.site s on (r.site_logical_name = s.logical_name and s.logical_name = %(spidername)s)
                join integration.work_site_mapping m on (r.work_uid = m.work_ori_id and m.site_id = s.id)
            """
        cursor.execute(sql, {'audit_id': audit_id, 'spidername': self.spidername})
        return cursor.rowcount


class LoadWorkSameAs(MyBasePostgresTask):
    """
    Used only when loading reviews from lt, to link same work together
    """
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()
    table = 'integration.WORK_SAMEAS'

    def requires(self):
        return BulkLoadReviews(self.spidername, self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work_sameas(work_uid, master_uid, create_dts, load_audit_id)
            select distinct cast(dup_uid as bigint), cast(work_uid as bigint), now(), %(audit_id)s
            from staging.review r
            where site_logical_name = 'librarything'
            and dup_uid is not null;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        return cursor.rowcount


class UpdateLtLastHarvest(MyBasePostgresTask):
    """
    Updates both work_uid and associated master work_uid (if any), so need to
    make sure update work_sameas was processed!
    """
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()
    table = 'integration.WORK'

    def requires(self):
        return {FetchLtWorkids('librarything', self.n_work, self.harvest_dts),
                LoadWorkSameAs('librarything', self.n_work, self.harvest_dts)}

    def exec_sql(self, cursor, audit_id):
        with self.input().open('r') as f:
            wids = tuple(parse_wids_file(f, True))
        sql = \
            """
            with all_uid as (
                select uid
                from integration.work
                where uid IN %(w_ids)s
                union
                select master_uid
                from integration.work_sameas
                where work_uid IN %(w_ids)s
                and master_uid IS NOT NULL
            )
            update integration.work w
            set last_harvest_dts = %(end)s
            from integration.work w
            join all_uid on all_uid.uid = w.uid;
            """
        cursor.execute(sql, {'end': self.harvest_dts, 'w_ids': wids})
        return cursor.rowcount


# python -m luigi --module brd.task BatchLoadReviews --spidername librarything --n-work 2 --harvest-dts 2016-01-31T190723 --local-scheduler
class BatchLoadReviews(luigi.Task):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    # for debug use, default should not be used otherwise different task will be launched even after failure
    harvest_dts = DateSecondParameter(default=datetime.datetime.now())

    global batch_name
    batch_name = "Reviews"  # for auditing

    def requires(self):
        requirements = [LoadReviews(self.spidername, self.n_work, self.harvest_dts),
                        UpdateOtherLastHarvestDate(self.spidername, self.n_work, self.harvest_dts)]
        if self.spidername == 'librarything':
            requirements.append(LoadReviews(self.spidername, self.n_work, self.harvest_dts),
                                UpdateLtLastHarvest(self.n_work, self.harvest_dts))
        return requirements


def parse_wids_file(wf, only_ids=False):
    list_wids = []
    for line in wf:
        jw = json.loads(line)
        if only_ids:
            list_wids.append(jw['work_uid'])
        else:
            list_wids.append(jw)
    return list_wids




# now Done during reviews...
class LoadWorkSiteMapping(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    site_logical_name = luigi.Parameter(default='librarything')

    table = 'integration.work_site_mapping'

    def requires(self):
        return [LoadWorkRef(self.thingisbn_filename)]

    def exec_sql(self, cursor, audit_id):
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
        cursor.execute(sql, {'audit_id': audit_id, 'logical_name': self.site_logical_name})
        return cursor.rowcount


class UpdateOtherLastHarvestDate(MyBasePostgresTask):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = DateSecondParameter()
    table = 'integration.WORK_SITE_MAPPING'

    def requires(self):
        return {FetchLtWorkids(self.spidername, self.n_work, self.harvest_dts)}

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
        cursor.execute(sql, {'end': self.harvest_dts, 'name': self.spidername, 'w_ids': wids})
        return cursor.rowcount

