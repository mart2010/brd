# -*- coding: utf-8 -*-
from brd.utils import get_all_files, get_column_headers

__author__ = 'mouellet'

import shutil
from brd.exception import EltError
import brd.db.dbutils as dbutils
import brd.utils as utils
import brd.config as config
import os
import psycopg2


class JobStatus():
    FAILED = 'Failed'
    COMPLETED = 'Completed'
    RUNNING = 'currently running..'


def treat_loaded_file(processed_file, remove, archive_dir):
    if remove:
        os.remove(processed_file)
    else:
        shutil.move(processed_file, archive_dir)


def bulkload_review_files(period=None, remove_scraped_file=False, truncate_staging=False):
    """
    Bulk loads Reviews*.dat files into staging. By default, load all files
    otherwise only the ones corresponding to period specified.
    Commit is done after each file loaded
    :param period: 'd-m-yyyy_d-m-yyyy'
    :return: (nb of files treated, nb of files with error)
    """
    pattern = "ReviewOf*.dat"
    if period:
        begin_period, end_period  = utils.resolve_period_text(period)
    if truncate_staging:
        truncate_table({'schema': 'staging', 'table': 'review'})
        dbutils.get_connection().commit()

    n_treated, n_error = 0, 0
    for datfile in get_all_files(config.SCRAPED_OUTPUT_DIR, pattern):
        file_begin, file_end = utils.resolve_period_text(datfile[datfile.index('_') + 1: datfile.index('.dat')])

        if period is None or (file_begin >= begin_period and file_end <= end_period):
            with open(datfile) as f:
                n_treated += 1
                audit_id = insert_auditing(job='Bulkload file', step=datfile, begin=file_begin, end=file_end)
                try:
                    count = copy_into_staging('review', get_column_headers(datfile), f)
                    update_auditing(commit=True, rows=count, status="Completed", id=audit_id)
                    treat_loaded_file(datfile, remove_scraped_file, config.SCRAPED_ARCHIVE_DIR)
                except psycopg2.DatabaseError, er:
                    dbutils.get_connection().rollback()
                    n_error += 1
                    insert_auditing(commit=True, job='Bulkload file', step=datfile, rows=0, status=er.pgerror, begin=file_begin, end=file_end)
                    # also add logging
                    print("Error bulk loading file: \n'%s'\nwith msg: '%s' " % (datfile, er.message))
    return (n_treated, n_error)


def launch_load_of_staged_reviews():
    """
    Prepare steps for loading staged reviews and launch complete process
    :return:
    """
    def step_dic(fct, no, params):
        return {'step_function': fct, 'step_no': no, 'step_params': params}

    batch_name = launch_load_of_staged_reviews.func_name
    steps = list()
    steps.append(step_dic(insert_into_reviewer, 1, {}))
    steps.append(step_dic(insert_into_book, 2, {}))
    steps.append(step_dic(insert_into_review, 3, {}))
    steps.append(step_dic(insert_into_book_site_review, 4, {}))
    steps.append(step_dic(truncate_table, 5, {'schema': 'staging', 'table': 'review'}))

    last_step_no, last_status = get_last_audit_steps(batch_name)

    if last_status == JobStatus.RUNNING:
        print ("Batch '%s' is still running, wait before launching new one" % batch_name)
    else:
        period_in_stage = fetch_periods_data_in_stage()
        if last_status == JobStatus.FAILED:
            if period_in_stage is None:
                raise EltError("Integrity error, previous Batch '%s' has failed but stage.review is empty" % batch_name)
            steps = steps[last_step_no - 1:]
        elif last_status == JobStatus.COMPLETED:
            if period_in_stage is None:
                raise EltError("Cannot launch Batch '%s' stage.review is empty" % batch_name)
        try:
            process_generic_steps(batch_name, period_in_stage, steps)
            print ("Finished processing Batch '%s!" % batch_name)
        except EltError as elt:
            print elt.message


def fetch_periods_data_in_stage():
    """
    Fetch all periods found in staging.review
    :return (min begin, max end) or None when table empty
    """
    sql = \
        """
        select min(period_begin), max(period_end)
        from staging.review r
        join staging.load_audit a on (a.id = r.load_audit_id);
        """
    res = dbutils.get_connection().fetch_one(sql)
    return res


def truncate_table(schema_table):
    sql = "truncate table %s.%s;" % (schema_table['schema'], schema_table['table'])
    dbutils.get_connection().execute(sql)



def get_last_audit_steps(batch):
    """
    :param batch name
    :return: (step_no, status) of last step logged or (-1, Completed) when no log is found
    """
    sql_last_step = \
        """
        select step_no, status
        from staging.load_audit as l
        where batch_job = %s
        and id = (select max(id) from staging.load_audit where batch_job = l.batch_job);
        """
    resp = dbutils.get_connection().fetch_one(sql_last_step, (batch,))
    if resp is None:
        return (-1, JobStatus.COMPLETED)
    last_step_no, last_status = resp

    if last_status.startswith(JobStatus.COMPLETED):
        last_status = JobStatus.COMPLETED
    elif last_status.startswith(JobStatus.FAILED):
        last_status = JobStatus.FAILED
    elif last_status.startswith(JobStatus.RUNNING):
        last_status = JobStatus.RUNNING
    else:
        raise EltError("Step no%d for batch '%s' has invalid status '%s'" % (last_step_no, batch, last_status))
    return (last_step_no, last_status)


def process_generic_steps(batch_name, period, steps_list):
    """
    Execute batch which is madeup of a number of steps function
    :param batch_name:
    :param period: 'd-m-yyyy_d-m-yyyy'
    :param steps_list: list of steps to be run in order and holding:
        {step_function: the_funct, step_no: run_order, step_params: (param1, ) }
    :return:
    """
    conn = dbutils.get_connection()
    # reset/rollback pending trx (RESET and SET SESSION AUTHORIZATION reset session to default)
    # conn.connection.reset()
    period_begin, period_end = period

    try:
        for step in steps_list:
            fct = step['step_function']
            params = step['step_params']
            params['audit_id'] = insert_auditing(job=batch_name,
                                                 step=fct.func_name,
                                                 step_no=step['step_no'],
                                                 begin=period_begin, end=period_end)
            # execute step function
            nb_rows = fct(params)
            update_auditing(commit=True, rows=nb_rows, status=JobStatus.COMPLETED, id=params['audit_id'])
    except psycopg2.DatabaseError, dbe:
        conn.rollback()
        msg = JobStatus.FAILED + ": Batch failed with DB error: '%s'" % (dbe.message)
        insert_auditing(commit=True, job=batch_name, status=msg, step=fct.func_name,
                        step_no=step['step_no'], begin=period_begin, end=period_end)
        raise EltError(msg, dbe)



def insert_auditing(commit=False, **named_params):
    sql = \
        """
        insert into staging.load_audit(batch_job, step_name, step_no, status, rows_impacted, period_begin, period_end, start_dts)
                            values (%(job)s, %(step)s, %(step_no)s, %(status)s, %(rows)s, %(begin)s, %(end)s, now());
        """
    assert('job' in named_params)
    assert('step' in named_params)
    assert('begin' in named_params)
    assert('end' in named_params)
    named_params['status'] = named_params.get('status', JobStatus.RUNNING)
    named_params['step_no'] = named_params.get('step_no', 0)
    named_params['rows'] = named_params.get('rows', -1)
    ret = dbutils.get_connection().insert_row_get_id(sql, named_params)
    if commit:
        dbutils.get_connection().commit()
    return ret


def update_auditing(commit=False, **named_params):
    sql = \
        """
        update staging.load_audit set status = %(status)s
                                    ,rows_impacted = %(rows)s
                                    ,finish_dts = now()
        where id = %(id)s;
        """
    assert('status' in named_params)
    assert('rows' in named_params)
    assert('id' in named_params)
    ret = dbutils.get_connection().execute(sql, named_params)
    if commit:
        dbutils.get_connection().commit()
    return ret


def copy_into_staging(tablename, columns, open_file):
    sql = \
        """
        copy staging.%s( %s )
        from STDIN with csv HEADER DELIMITER '|' NULL '';
        """ % (tablename, columns)
    return dbutils.get_connection().copy_expert(sql, open_file)


def insert_into_reviewer(named_params):
    # ok since now() always return timestamp as of begining of transaction
    sql = \
        """
        insert into integration.reviewer(id, site_id, pseudo, load_audit_id, create_dts)
        select
            integration.derive_reviewerid(r.reviewer_pseudo, r.site_logical_name) as reviewerid
            , s.id
            , r.reviewer_pseudo
            , %(audit_id)s
            , now()
        from staging.review r
        join integration.site s on (r.site_logical_name = s.logical_name)
        except
        select
            id
            , site_id
            , pseudo
            , %(audit_id)s
            , now()
        from integration.reviewer;
        """
    return dbutils.get_connection().execute(sql, named_params)


def insert_into_book(named_params):
    sql = \
        """
        insert into integration.book(id, title_sform, lang_code, author_sform, create_dts, load_audit_id)
        select * from
        (select
             integration.derive_bookid( integration.get_sform(r.book_title),
                                        r.book_lang,
                                        integration.standardize_authorname(r.author_fname, r.author_lname) ) as bookid
             , integration.get_sform(r.book_title) as title_sform
             , r.book_lang
             , integration.standardize_authorname(r.author_fname, r.author_lname) as author_sform
             , now()
             , %(audit_id)s
        from staging.review r
        group by 1, 2, 3, 4
        ) as new_book
        where
        NOT EXISTS ( select b.id
                     from integration.book b
                     where b.id = new_book.bookid
                    );
        """
    return dbutils.get_connection().execute(sql, named_params)

def insert_into_review(named_params):
    sql = \
        """
        insert into integration.review(book_id, reviewer_id, review_date, rating_code, create_dts, load_audit_id)
        select
            integration.derive_bookid( integration.get_sform(r.book_title),
                                       r.book_lang,
                                       integration.standardize_authorname(r.author_fname, r.author_lname) ) as bookid
            , integration.derive_reviewerid( r.reviewer_pseudo, r.site_logical_name ) as reviewerid
            , r.parsed_review_date
            , r.review_rating
            , now()
            , %(audit_id)s
        from staging.review r
        """
    return dbutils.get_connection().execute(sql, named_params)


def insert_into_book_site_review(named_params):
    sql = \
        """
        insert into integration.book_site_review(book_id, site_id, book_uid, title_text, create_dts, load_audit_id)
        select distinct
            integration.derive_bookid( integration.get_sform(r.book_title),
                                       r.book_lang,
                                       integration.standardize_authorname(r.author_fname, r.author_lname) ) as bookid
            , s.id
            , r.book_uid
            , r.book_title
            , now()
            , %(audit_id)s
        from staging.review r
        join integration.site s on (r.site_logical_name = s.logical_name)
        where
        NOT EXISTS (    select bsr.book_id
                        from integration.book_site_review bsr
                        where bsr.site_id = s.id
                        and bsr.book_id = integration.derive_bookid( integration.get_sform(r.book_title),
                                                                     r.book_lang,
                                                                     integration.standardize_authorname(r.author_fname, r.author_lname) )
                    );
        """
    return dbutils.get_connection().execute(sql, named_params)



def insert_into_review_rejected(named_params):
    sql = \
        """
        insert into staging.review_rejected
        select *
        from staging.review r
        left join integration.book b on (r.derived_book_id = b.id)
        where b.id IS NULL;
        """
    return dbutils.get_connection().execute(sql, named_params)

