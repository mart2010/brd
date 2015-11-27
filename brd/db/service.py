# -*- coding: utf-8 -*-
__author__ = 'mouellet'

import shutil
from datetime import datetime
import brd.db.dbutils as dbutils
import brd.config as config
import os
import fnmatch
import brd.scrapy.utils as utils
import psycopg2


def get_pending_files(repdir, pattern):
    """
    Get all files under repdir and below (recursively)
    :return: list of files with matching pattern
    """
    if not os.path.lexists(repdir):
        raise EnvironmentError("Invalid directory defined :'" + repdir + "'")

    pending_files = []
    for root, dirnames, filenames in os.walk(repdir):
        for filename in fnmatch.filter(filenames, pattern):
            pending_files.append(os.path.join(root, filename))
    return pending_files


def get_column_headers(file_with_header):
    """
    Read the column headers as stored in file
    (by default scrapy exports fields in any order, i.e. dict-like)
    :param file_with_header:
    :return: string of column headers with correct order
    """
    with open(file_with_header, 'r') as f:
        h = f.readline().split("|")
    return ",".join(h).strip()


def treat_loaded_file(processed_file, remove, archive_dir):
    if remove:
        os.remove(processed_file)
    else:
        shutil.move(processed_file, archive_dir)


def bulkload_review_files(period=None, remove=False):
    """
    Bulk loads Reviews*.dat files under reviews_root dir into staging
    By default, load all files otherwise only the ones corresponding to period
    :param period: 'd-m-yyyy_d-m-yyyy'
    :return: (nb of files treated, nb of files with error)
    """
    pattern = "ReviewOf*.dat"
    if period:
        begin_period, end_period  = utils.resolve_period_text(period)

    n_treated, n_error = 0, 0
    for datfile in get_pending_files(config.SCRAPED_OUTPUT_DIR, pattern):
        file_begin, file_end = utils.resolve_period_text(datfile[datfile.index('_') + 1: datfile.index('.dat')])

        if file_begin >= begin_period and file_end <= end_period:
            n_treated += 1
            with open(datfile) as f:
                audit_id = load_auditing({'job': 'Bulkload file', 'step': datfile, 'begin': begin_period, 'end': end_period})
                try:
                    count = bulkload_into_staging('review', get_column_headers(datfile), f)
                    update_auditing({'nb': count, 'status': "Completed", 'id': audit_id})
                    dbutils.get_connection().commit()
                    treat_loaded_file(datfile, remove, config.SCRAPED_ARCHIVE_DIR)
                except psycopg2.DatabaseError, er:
                    dbutils.get_connection().rollback()
                    n_error += 1
                    a_id = load_auditing({'job': 'Bulkload file', 'step': datfile, 'begin': begin_period, 'end': end_period})
                    update_auditing({'nb': 0, 'status': er.pgerror, 'id': a_id})
                    dbutils.get_connection().commit()
                    # also add logging
                    print("Error bulk loading file: \n'%s'\nwith msg: '%s' " % (datfile, er.message))

    return (n_treated, n_error)


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
    :param period: 'd-m-yyyy_d-m-yyyy'
    :param steps: list of tuple containing (function, params)
    :return:
    """
    conn = dbutils.get_connection()
    period_begin, period_end = utils.resolve_period_text(period)

    try:

        for step in steps:
            fct = step[0]
            params = step[1]

            # see if auditing can be designed as a decorator added to all fcts involved (issues with batch_name and period params)
            audit_id = load_auditing({'job': batch_name, 'step': fct.func_name, 'begin': period_begin, 'end': period_end})

            # execute step function
            params['audit_id'] = audit_id
            nb_rows = fct(params)

            update_auditing({'nb': nb_rows, 'status': "Completed", 'id': audit_id})

        conn.commit()
    # TODO: replace custom error
    except ValueError, err:
        conn.rollback()
        msg = "Batch '%s' failed at step '%s' with error msg: %s" % (batch_name, fct.func_name, err.message)
        load_auditing({'job': batch_name, 'status': msg, 'step': fct.func_name, 'begin': period_begin, 'end': period_end})
        conn.commit()



def start_load_review_batch():
    """
    Reset transaction and fetch all periods currently stored in staging.review
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


def load_auditing(named_params):
    sql = \
        """
        insert into staging.load_audit(batch_job, step_name, status, period_begin, period_end, start_dts)
                            values (%(job)s, %(step)s, 'currently processing...', %(begin)s, %(end)s, now())
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


def bulkload_into_staging(tablename, columns, open_file):
    sql = \
        """
        copy staging.%s( %s )
        from STDIN with csv HEADER DELIMITER '|' NULL ''
        """ % (tablename, columns)
    return dbutils.get_connection().copy_expert(sql, open_file)


def load_review(named_params):
    sql = \
        """
        insert into integration.review(book_id, reviewer_id, review_date, rating_code, create_dts, load_audit_id)
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
        insert into integration.book_site_review(book_id, site_id, book_uid, title_text, create_dts, load_audit_id)
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
        insert into integration.reviewer(id, site_id, pseudo, create_dts, load_audit_id)
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

