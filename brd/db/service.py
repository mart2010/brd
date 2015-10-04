# -*- coding: utf-8 -*-
__author__ = 'mouellet'

import datetime
import brd.db.dbutils as dbutils



def persist_scraped_review(period):
    """
    Persist a new review into staging (pipeline should call this instead..?)
    Here.. I should actually trigger scraping session with each spiders and store auditing meta related to...
    :return:
    """
    pass


def load_staged_reviews():
    """
    Prepare steps for loading staged reviews and launch process
    :return:
    """
    batch_name = load_staged_reviews.func_name

    period = start_load_review_batch()
    steps = list()
    # step
    steps.append((load_review, {}))
    # step
    steps.append((load_book_site_review, {}))
    # step
    steps.append((load_reviewer, {}))

    process_all_steps(batch_name, period, steps)

    complete_load_review_batch()


def process_all_steps(batch_name, period, steps):
    """
    Execute batch madeup of a number of steps function
    :param batch_name:
    :param period: tuple containing (begin_date, end_date)
    :param steps: list of tuple containing (function, params)
    :return:
    """
    conn = dbutils.get_connection()
    period_begin, period_end = period

    try:

        for step in steps:
            fct = step[0]
            params = step[1]

            # see if auditing can be designed as a decorator added to all fcts involved (issues with batch_name and period params)
            audit_id = create_auditing({'job': batch_name, 'step': fct.func_name, 'begin': period_begin, 'end': period_end})

            # execute the step function
            params['audit_id'] = audit_id
            nb_rows = fct(params)

            update_auditing({'nb': nb_rows, 'status': "Completed", 'id': audit_id})

        conn.commit()
    # TODO: this will be replaced by my error!!
    except ValueError, err:
        conn.rollback()
        msg = "Batch '%s' failed at step '%s' with error msg: %s" % (batch_name, fct.func_name, err.message)
        create_auditing({'job': batch_name, 'status': msg, 'step': fct.func_name, 'begin': period_begin, 'end': period_end})
        conn.commit()



def start_load_review_batch():
    """
    Reset transaction and fetch the period date of all staged reviews
    :return period as tuple (begin_date, end_date)
    """
    sql = \
        """
        select min(period_begin), max(period_end)
        from staging.review r
        join integration.load_audit a on (a.id = r.load_audit_id)
        """
    conn = dbutils.get_connection()

    # rollback pending transaction (RESET and SET SESSION AUTHORIZATION reset session to default)
    conn.connection.reset()
    res = conn.fetch_one(sql)
    if res is None:
        # TO-DO: create my own ETL-type exception with dedicated  messages...
        raise ValueError("No data in stage.review, stop processsing")
    elif type(res[0]) is not datetime.date:
        raise ValueError("Period date should be of type datetime.date")
    else:
        return res


def complete_load_review_batch():
    """
    Truncate the staged review
    """
    sql = \
        """
        truncate table staging.review
        """
    conn = dbutils.get_connection()
    conn.execute_inTransaction(sql)


def create_auditing(named_params):
    sql = \
        """
        insert into staging.load_audit(batch_job, step_name, period_begin, period_end, start_dts)
                            values (%(job)s, %(step)s, %(begin)s, %(end)s, now())
        """

    return dbutils.get_connection().insert_row_get_id(sql, named_params)


def update_auditing(named_params):
    sql = \
        """
        update staging.load_audit set status = %(status)s
                                    ,rows_impacted = %(nb)s
                                    ,finish_dts = now()
        where id = %(id)s
        """
    return dbutils.get_connection().execute(sql, named_params)




def load_review(named_params):
    sql = \
        """
        insert into integration.review( book_id, reviewer_id, review_date, rating_code, create_dts, load_audit_id )
        select
            b.id
            , reviewer_id
            , derived_review_date
            , review_rating
            , now()
            , %(audit_id)s
        from staging.review r
        join integration.book b on (r.derived_book_id = b.id)
        """
    return dbutils.get_connection().execute(sql, named_params)


def load_book_site_review(named_params):
    sql = \
        """
        insert into integration.book_site_review( book_id, site_id, book_uid, title_text, create_dts, load_audit_id )
        select distinct
            b.id
            , s.id
            , r.book_uid
            , r.title_text
            , now()
            , %(audit_id)s
        from staging.review r
        join integration.site s on (r.site_logical_name = s.logical_name)
        where
        NOT EXISTS (
                        select bsr.book_id
                        from integration.book_site_review bsr
                        where bsr.site_id = s.id
                        bsr.book_id = r.derived_book_id
                    )
        """
    return dbutils.get_connection().execute(sql, named_params)


def load_reviewer(named_params):
    sql = \
        """
        insert into integration.reviewer( id, site_id, pseudo, create_dts, load_audit_id)
        select
            b.id
            , s.id
            , rere
            , %(audit_id)s
        from staging.review r
        join integration.site s on (r.site_id = s.id)
        where
        """
    return dbutils.get_connection().execute(sql, named_params)


def load_review_rejected(named_params):
    sql = \
        """
        insert into staging.review_rejected
        select *
        from staging.review r
        left join integration.book b on (r.derived_book_id = b.id)
        where b.id IS NULL
        """
    return dbutils.get_connection().execute(sql, named_params)

