import datetime
import json
import logging

import brd
import brd.service as service
import luigi
import os
from brd.taskbase import BaseBulkLoadTask, BasePostgresTask, batch_name

logger = logging.getLogger(__name__)

class FetchNewWorkIds(luigi.Task):
    """
    Fetch n_work NOT yet harvested for site and their associated isbns
    Used before harvesting sites where the work-uid is unknown.
    """
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()

    def output(self):
        wids_filepath = '/tmp/newwids_%s_%s.txt' % \
                        (self.site, self.harvest_dts.strftime(luigi.DateMinuteParameter.date_format))
        return luigi.LocalTarget(wids_filepath)

    def run(self):
        f = self.output().open('w')
        res_dic = service.fetch_workIds_not_harvested(self.site, nb_work=self.n_work)
        json.dump(res_dic, f, indent=2)
        f.close()

class HarvestReviews(luigi.Task):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()

    def __init__(self, *args, **kwargs):
        super(HarvestReviews, self).__init__(*args, **kwargs)

        filename = 'ReviewsOf%s_%s.csv' % (self.site,
                                           self.harvest_dts.strftime(luigi.DateMinuteParameter.date_format))
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
                                                    works_to_harvest=workids_list,
                                                    to_date=self.harvest_dts)
        spider_process.start_process()
        logger.info("Harvest of %d works/review completed with spider %s (dump file: '%s')"
                     % (len(workids_list), self.site, self.dump_filepath))


class BulkLoadReviews(BaseBulkLoadTask):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()

    input_has_headers = True
    clear_table_before = True
    table = 'staging.REVIEW'

    def requires(self):
        return HarvestReviews(self.site, self.n_work, self.harvest_dts)


class LoadUsers(BasePostgresTask):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()

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
                and user_uid is not null
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
        return cursor.rowcount


class LoadReviews(BasePostgresTask):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
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


class LoadLtWorkSameAs(BasePostgresTask):
    """
    Used with  lt.  This should not happen since this is also done during harvest of work-info.
    However we leave as it could still happen that during work-info harvest duplicates still existed..
    """
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.WORK_SAMEAS'

    def requires(self):
        return BulkLoadReviews('librarything', self.n_work, self.harvest_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into integration.work_sameas(work_refid, master_refid, create_dts, load_audit_id)
            select distinct dup_refid, work_refid, now(), %(audit_id)s
            from staging.review r
            where site_logical_name = 'librarything'
            and dup_refid is not null;
            """
        cursor.execute(sql, {'audit_id': audit_id})
        c = cursor.rowcount
        logger.warning("Found %s duplicates during lt harvest review (audit_id=%s)" %(str(c), str(audit_id)))
        return c


class UpdateLtLastHarvest(BasePostgresTask):
    """
    Updates both work_refid and associated master work_refid (if any), so
    make sure update work_sameas was processed!
    (note: for other site, this gets done in task LoadWorkSiteMapping)
    """
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()
    table = 'integration.WORK'

    def requires(self):
        return {'wids': FetchNewWorkIds('librarything', self.n_work, self.harvest_dts),
                'same': LoadLtWorkSameAs(self.n_work, self.harvest_dts)}

    def exec_sql(self, cursor, audit_id):
        with self.input()['wids'].open('r') as f:
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


class LoadWorkSiteMapping(BasePostgresTask):
    """
    Required for site that maps work through reviews harvesting
    (this also updates last_harvest_dts).
    """
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    harvest_dts = luigi.DateMinuteParameter()

    table = 'integration.WORK_SITE_MAPPING'

    def requires(self):
        return [LoadReviews(self.site, self.n_work, self.harvest_dts)]

    def exec_sql(self, cursor, audit_id):
        # work_uid=-1 means no work could be found for isbns...
        sql = \
            """
            insert into integration.work_site_mapping(work_refid, work_uid, site_id, title, authors,
                last_harvest_dts, create_dts, load_audit_id)
            select  work_refid,
                    coalesce(work_uid, '-1'),
                    (select id from integration.site where logical_name = %(logical_name)s),
                    title, authors,
                    %(harvest_dts)s, now(), %(audit_id)s
            from
            staging.review
            where site_logical_name = %(logical_name)s
            """
        cursor.execute(sql, {'audit_id': audit_id,
                             'logical_name': self.site,
                             'harvest_dts': self.harvest_dts})
        return cursor.rowcount


# python -m luigi --module brd.task BatchLoadReviews --site librarything
# --n-work 2 --harvest-dts 2016-01-31T1907 --local-scheduler

class BatchLoadReviews(luigi.Task):
    site = luigi.Parameter()
    n_work = luigi.IntParameter()
    # default values trigger different task even after failure!
    harvest_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())

    global batch_name
    batch_name = "Reviews"  # for auditing

    def requires(self):
        reqs = [LoadReviews(self.site, self.n_work, self.harvest_dts)]
        if self.site == 'librarything':
            reqs.append(UpdateLtLastHarvest(self.n_work, self.harvest_dts))
        else:
            reqs.append(LoadWorkSiteMapping(self.site, self.n_work, self.harvest_dts))
        return reqs