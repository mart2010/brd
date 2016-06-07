import logging

import datetime

import luigi
from brd.taskbase import BasePostgresTask

__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The BRD Project"

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------------- #
# -----------------------------  LOAD REVIEW_SIMILAR_TO --------------------------------------- #
# --------------------------------------------------------------------------------------------- #

class LoadRevSimilarToProcess(BasePostgresTask):
    """
    Loads new work/review-ids into rev_similarto_process in sequential order.

    Only loads work not yet processed (not adapted for incremental reviews
    created after work was last processed)
    """
    n_work = luigi.IntParameter()
    process_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())
    table = 'integration.REV_SIMILARTO_PROCESS'


    # TODO: vaidate the new rules that take only work_refid harvested for 3 sites (using work and work_site_mapping instead!!!)
    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into {table}(work_refid, review_id, text_length, review_lang, create_dts, load_audit_id)
            select rev.work_refid, id, coalesce(char_length(review),0), review_lang, now(), %(audit_id)s
            from integration.review rev
            join (select distinct work_refid
                  from integration.work w
                  join work_site_mapping wg on (wg.work_refid = w.refid and wg.site_id = 2 and w.last_harvest_dts IS NOT NULL)
                  join work_site_mapping wb on (wb.work_refid = wg.work_refid and wb.site_id = 4)
                  where not exists (select work_refid
                                    from {table} rs
                                    where rs.work_refid = w.refid)
                  and w.last_harvest_dts IS NOT NULL
                  order by 1
                  limit %(n_work)s) as new_work
            on new_work.work_refid = rev.work_refid
            where
            review_lang not in ('--','und');
            """.format(table=self.table)
        cursor.execute(sql, {'n_work': self.n_work, 'audit_id': audit_id})
        return cursor.rowcount

class CreateTempRevProcess(BasePostgresTask):
    n_work = luigi.IntParameter()
    process_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())
    table = 'staging.TMP_REVPROCESS'

    # process reviews with minimum nb of char and of similar text length
    length_min = 100
    length_delta = 0.08

    def requires(self):
        return LoadRevSimilarToProcess(self.n_work, self.process_dts)

    def exec_sql(self, cursor, audit_id):
        # process only new batch of work (rev.date_processed IS NULL)
        sql = \
            """
            create table {table}_{dt} as
                select rev.work_refid, rev.review_id as id, r.review, r.site_id, other.review_id as other_id,
                        o.review as other_review, o.site_id as other_site_id, similarity(r.review, o.review), %(audit_id)s
                from integration.rev_similarto_process rev
                join integration.review r on (rev.review_id = r.id)
                join integration.rev_similarto_process other on
                    (rev.work_refid = other.work_refid
                     and rev.review_lang = other.review_lang
                     and rev.review_id > other.review_id
                     and rev.text_length between other.text_length - round(other.text_length * %(len_delta)s) and
                                                 other.text_length + round(other.text_length * %(len_delta)s))
                join integration.review o on (other.review_id = o.id)
                where
                rev.date_processed IS NULL
                and rev.text_length >= %(len_min)s
                and other.text_length >= %(len_min)s
            """.format(table=self.table, dt=self.process_dts.strftime('%Y_%m_%dT%H%M'))
        cursor.execute(sql, {'len_min': self.length_min, 'len_delta': self.length_delta, 'audit_id': audit_id})
        return cursor.rowcount

class LoadReviewSimilarTo(BasePostgresTask):
    n_work = luigi.IntParameter()
    process_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())
    table = 'integration.REVIEW_SIMILARTO'

    # min similarity index for two reviews to be considered similar
    similarity_min = 0.6

    def requires(self):
        return CreateTempRevProcess(self.n_work, self.process_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into {table}(site_id, review_id, other_site_id, other_review_id, similarity, create_dts, load_audit_id)
            select site_id, s.id, other_site_id, other_id, similarity, now(), %(audit_id)s
            from {source}_{dt} s
            join (select id, min(other_id) o_id
                  from {source}_{dt}
                  where similarity >= %(sim)s
                  group by 1) t on (s.id = t.id and s.other_id = t.o_id)
            where similarity >= %(sim)s
            """.format(table=self.table, source=CreateTempRevProcess.table, dt=self.process_dts.strftime('%Y_%m_%dT%H%M'))
        cursor.execute(sql, {'sim': self.similarity_min, 'audit_id': audit_id})


class UpdateReviewSimilarToProcess(BasePostgresTask):
    n_work = luigi.IntParameter()
    process_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())
    table = 'integration.REV_SIMILARTO_PROCESS'

    def requires(self):
        return LoadReviewSimilarTo(self.n_work, self.process_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            update {table} set date_processed = %(dts)s
            where date_processed IS NULL
            """.format(table=self.table)
        cursor.execute(sql, {'dts': self.process_dts})


# python -m luigi --module brd.taskpres BatchProcessReviewSimilarTo --n-work 50 --process-dts 2016-05-26T1200  --local-scheduler
class BatchProcessReviewSimilarTo(BasePostgresTask):
    """
    Entry point to launch batch and clear temporary table..
    """
    n_work = luigi.IntParameter()
    process_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())
    table = CreateTempRevProcess.table

    def requires(self):
        return [UpdateReviewSimilarToProcess(self.n_work, self.process_dts)]

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            drop table {tmp}_{dt};
            """.format(tmp=self.table, dt=self.process_dts.strftime('%Y_%m_%dT%H%M'))
        cursor.execute(sql, {'dts': self.process_dts})


import argparse
import subprocess

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    # mandatory argument is name of task
    parser.add_argument('task', help="Name of task (only 'BatchProcessReviewSimilarTo' supported for now)")
    parser.add_argument('-t', '--nbtask', default=1, type=int, help="Number of tasks to execute sequentially")
    # too low nbwork result in skipping tasks (take less than 1 min so following task have same process_dts)
    # too high yields too large result due to cross-product
    parser.add_argument('-w', '--nbwork', default=100, type=int, help="Number of works to process by task")
    parser.add_argument('--caffeinate', action="store_true", help="Optionally prefix with caffeinate command")
    args = parser.parse_args()

    if args.task != 'BatchProcessReviewSimilarTo':
        raise RuntimeError("Stop script, only 'BatchProcessReviewSimilarTo' can be launched")

    cmd = []
    if args.caffeinate:
        cmd.extend(['caffeinate', '-i'])
    cmd.extend(['python', '-m'])
    cmd.extend(['luigi', '--module'])
    cmd.extend(['brd.taskpres', args.task])
    cmd.extend(['--n-work', str(args.nbwork)])
    cmd.append('--local-scheduler')
    logger.info("Command '%s' will be executed %d times" % (" ".join(cmd), args.nbtask))
    for i in xrange(args.nbtask):
        subprocess.check_call(cmd)



