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

class LoadReviewSimilarToProcess(BasePostgresTask):
    """
    This loads new work/reviews into rev_similarto_process in sequential order.

    Only loads work not yet processed, not adapted for incremental reviews
    (new reviews loaded since work was last processed)
    """
    n_work = luigi.IntParameter()
    process_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())
    table = 'integration.REV_SIMILARTO_PROCESS'

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            insert into {table}(work_refid, review_id, text_length, review_lang,
                                site_id, create_dts, load_audit_id)
            select rev.work_refid, id, char_length(review), review_lang, site_id, now(), %(audit_id)s
            from integration.review rev
            join (select distinct work_refid
                  from integration.review r
                  where not exists (select work_refid
                                    from {table} rs
                                    where rs.work_refid = r.work_refid)
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
    length_delta = 0.10

    def requires(self):
        return LoadReviewSimilarToProcess(self.n_work, self.process_dts)

    def exec_sql(self, cursor, audit_id):
        # process only new batch of work (rev.date_processed IS NULL)
        sql = \
            """
            create table {table}_{dt} as
                select rev.work_refid, rev.review_id as id, r.review, other.review_id as other_id,
                        o.review as other_review, similarity(r.review, o.review), %(audit_id)s
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


class UpdateReviewSimilarToProcess(BasePostgresTask):
    n_work = luigi.IntParameter()
    process_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())
    table = 'integration.REV_SIMILARTO_PROCESS'

    def requires(self):
        return CreateTempRevProcess(self.n_work, self.process_dts)

    def exec_sql(self, cursor, audit_id):
        sql = \
            """
            update {table} set date_processed = %(dts)s
            where date_processed IS NULL
            """.format(table=self.table)
        cursor.execute(sql, {'dts': self.process_dts})


# python -m luigi --module brd.taskpres BatchProcessReviewSimilarTo --n-work 2 --process-dts 2016-05-26T1200  --local-scheduler
class BatchProcessReviewSimilarTo(luigi.Task):
    """
    Entry point to launch batch
    """
    n_work = luigi.IntParameter()
    process_dts = luigi.DateMinuteParameter(default=datetime.datetime.now())

    global batch_name
    batch_name = "Process similarTo review"  # for auditing

    def requires(self):
        return [UpdateReviewSimilarToProcess(self.n_work, self.process_dts)]


