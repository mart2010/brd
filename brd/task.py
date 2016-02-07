
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
    Provide to subclass the needed operations to read/write from the DB
    and make them valid as luigi's task
    Subclass must provide the target table (self.table) and implement sql load logic
    in exec_sql().
    """

    def output(self):
        return postgres_target(self.table, self.task_id)

    def run(self):
        connection = self.output().connect()

        # I could even add my audit-log stuff before and after here (to get elapse time and auditing infor)
        # here I would pass all param receive by the task...
        # check with get_param_values()
        ret = self.exec_sql(connection)

        # mark as complete in same transaction (checkpoint)
        self.output().touch(connection)
        # commit and clean up
        connection.commit()
        connection.close()

    # to be implemented by subclass
    def exec_sql(self, connection):
        raise NotImplementedError



class DownLoadThingISBN(luigi.Task):
    filepath = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.filepath)


class BulkLoadThingISBN(luigi.postgres.CopyToTable):
    """
    This task bulkload thingisbn file into staging
    TODO: truncate staging table first not to accumulate older reference
    """
    thingisbn_filename = luigi.Parameter()

    # TODO: annoying ... can't instantiate without (cannot use my postgre_target()  as these attributes set as abstractproperty in rdbms.CopyToTable)
    host = brd.config.DATABASE['host']
    database = brd.config.DATABASE['database']
    user = brd.config.DATABASE['user']
    password = brd.config.DATABASE['password']
    table = 'staging.thingisbn'

    columns = ["WORK_UID", "ISBN_ORI", "ISBN10", "ISBN13", "LOAD_AUDIT_ID"]
    column_separator = "|"

    def requires(self):
        fullpath_name = os.path.join(brd.config.REF_DATA_DIR, self.thingisbn_filename)
        return DownLoadThingISBN(fullpath_name)


class LoadWorkRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    table = 'integration.work'

    def requires(self):
        return BulkLoadThingISBN(self.thingisbn_filename)

    def exec_sql(self, connection):
        sql = \
            """
            insert into integration.work(uid, create_dts)
            select distinct s.work_uid, now()
            from staging.thingisbn s
            left join integration.work w on s.work_uid = w.uid
            where w.uid is null;
            """
        return connection.cursor().execute(sql)


class LoadIsbnRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    table = 'integration.isbn'

    def requires(self):
        return BulkLoadThingISBN(self.thingisbn_filename)

    def exec_sql(self, connection):
        sql = \
            """
            insert into integration.isbn(ean, isbn13, isbn10, create_dts)
            select distinct cast(s.isbn13 as bigint), s.isbn13, s.isbn10, now()
            from staging.thingisbn s
            left join integration.isbn i on cast(s.isbn13 as bigint) = i.ean
            where i.ean is null;
            """
        return connection.cursor().execute(sql)


class LoadWorkIsbnRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    table = 'integration.work_isbn'

    def requires(self):
        return [LoadWorkRef(self.thingisbn_filename), LoadIsbnRef(self.thingisbn_filename)]

    def exec_sql(self, connection):
        sql = \
            """
            insert into integration.work_isbn(ean, work_uid, source_site_id, create_dts)
            select distinct cast(s.isbn13 as bigint), s.work_uid, (select id from integration.site where logical_name = 'librarything')
                    , now()
            from staging.thingisbn s
            left join integration.work_isbn i on (cast(s.isbn13 as bigint) = i.ean and s.work_uid = i.work_uid)
            where i.ean is null;
            """
        return connection.cursor().execute(sql)


class LoadWorkSiteMappingRef(MyBasePostgresTask):
    thingisbn_filename = luigi.Parameter()
    site_logical_name = luigi.Parameter(default='librarything')

    table = 'integration.work_site_mapping'

    def requires(self):
        return [LoadWorkRef(self.thingisbn_filename)]

    def exec_sql(self, connection):
        sql = \
            """
            insert into integration.work_site_mapping(ref_uid, work_id, site_id, work_ori_id, create_dts)
            select work.uid, integration.derive_other_work_id(work.uid::text,%(logical_name)s)
                    , work.site_id, work.uid_text, now()
            from
            (select uid, uid::text as uid_text, (select id from integration.site where logical_name = %(logical_name)s) as site_id
            from integration.work) as work
            left join integration.work_site_mapping m on (work.uid_text = m.work_ori_id and m.site_id = work.site_id)
            where m.work_ori_id IS NULL;
            """
        return connection.cursor().execute(sql, {'logical_name': self.site_logical_name})


batch_name = None   # ok, as long as concurrent batch job launched in separate process

class BatchLoadWorkReference(luigi.Task):
    """
    Entry point to launch all loads related to 'thingisbn.csv' filen
    """
    thingisbn_filename = luigi.Parameter(default='thingISBN_10000_26-1-2016.csv')

    global batch_name
    batch_name = "Batch: load reference thingisbn" # for auditing

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
        return luigi.LocalTarget(self.dump_filepath)

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
    table = 'integration.work_site_mapping'

    def requires(self):
        return {'wids': FetchWorkids(self.spidername, self.n_work, self.harvest_dts),
                'harvest': HarvestReviews(self.spidername, self.n_work, self.harvest_dts)}

    def exec_sql(self, connection):
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
        r = connection.cursor().execute(sql, {'end': self.harvest_dts,
                                              'name': self.spidername,
                                              'w_ids': wids})

# python -m luigi --module brd.task BatchLoadReviews --spidername librarything --n-work 2 --harvest-dts 2016-01-31T190723 --local-scheduler
class BatchLoadReviews(luigi.Task):
    spidername = luigi.Parameter()
    n_work = luigi.IntParameter()
    # for debug use, default should not be used otherwise different task will be launched even after failure
    harvest_dts = DateSecondParameter(default=datetime.datetime.now())

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

