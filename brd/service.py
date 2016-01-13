# -*- coding: utf-8 -*-
import shutil

import brd
from brd import get_all_files, get_column_headers
import brd.elt as elt
from brd.elt import BatchProcessor, Step
import brd.config as config
import os

__author__ = 'mouellet'


def treat_loaded_file(processed_file, remove, archive_dir):
    if remove:
        os.remove(processed_file)
    else:
        shutil.move(processed_file, archive_dir)


def bulkload_thingisbn(remove_file=False, truncate_staging=True):
    """
    Try to load one reference file: thingISBN*_d-m-yyyy.csv
    :param remove_file:
    :param truncate_staging:
    :return:
    """

    pattern = "thingISBN_10*.csv"
    file_to_load = get_all_files(config.REF_DATA_DIR, pattern, recursively=False)

    if len(file_to_load) == 1:
        file_to_load = file_to_load[0]
    else:
        raise elt.EltError("Expected ONE thingISBN file to load: %s" % str(file_to_load))

    file_date = file_to_load[file_to_load.rindex('_') + 1:file_to_load.rindex('.csv')]
    # thingISBN data is a one-time snapshot (no period of validity, i.e. period_begin = period_end)
    period_begin  = brd.resolve_onedate_text(file_date)

    if truncate_staging:
        elt.truncate_table({'schema': 'staging', 'table': 'thingisbn'}, True)
    n = elt.bulkload_file(file_to_load, 'thingisbn', 'WORK_UID, ISBN, LOAD_AUDIT_ID', (period_begin, period_begin))
    if n != -1:
        treat_loaded_file(file_to_load, remove_file, config.REF_ARCHIVE_DIR)



def bulkload_review_files(period=None, remove_files=False, truncate_staging=False):
    """
    Bulk loads Reviews*.dat files into staging DB. By default, load all files
    otherwise only the ones corresponding to period specified.
    Commit is done after each file loaded
    :param period: 'd-m-yyyy_d-m-yyyy'
    :return: (nb of files treated, nb of files with error)
    """
    pattern = "ReviewOf*.dat"
    if period:
        begin_period, end_period  = brd.resolve_period_text(period)
    if truncate_staging:
        elt.truncate_table({'schema': 'staging', 'table': 'review'}, True)

    n_treated, n_error = 0, 0
    for datfile in get_all_files(config.SCRAPED_OUTPUT_DIR, pattern, True):
        file_begin, file_end = brd.resolve_period_text(datfile[datfile.index('_') + 1: datfile.index('.dat')])

        if period is None or (file_begin >= begin_period and file_end <= end_period):
            n = elt.bulkload_file(datfile, 'review', get_column_headers(datfile), (file_begin, file_end))
            if n == -1:
                n_error += 1
            else:
                n_treated += 1
                treat_loaded_file(datfile, remove_files, config.SCRAPED_ARCHIVE_DIR)
    return (n_treated, n_error)



def batch_loading_workisbn(truncate_stage=False):
    """

    :return:
    """
    batch = BatchProcessor(batch_loading_workisbn.func_name, 'thingisbn')
    batch.add_step(Step(name="load_work",
        sql="""
        insert into integration.work(uid, load_audit_id, create_dts)
        select distinct s.work_uid
            , %(audit_id)s
            , now()
        from staging.thingisbn s
        left join integration.work w on s.work_uid = w.uid
        where w.uid is null;
        """))

    batch.add_step(Step(name="load_isbn",
        sql="""
        insert into integration.isbn(isbn10, load_audit_id, create_dts)
        select distinct s.isbn
            , %(audit_id)s
            , now()
        from staging.thingisbn s
        left join integration.isbn i on s.isbn = i.isbn10
        where i.isbn10 is null;
        """))

    # TODO: design for possible deletion of work_uid/isbn (relevant also for work and isbn table) ?
    batch.add_step(Step(name="load_work_isbn",
        sql="""
        insert into integration.work_isbn(isbn10, work_uid, source_site_id, load_audit_id, create_dts)
        select distinct s.isbn
            , s.work_uid
            , (select id from integration.site where logical_name = %(logical_name)s)
            , %(audit_id)s
            , now()
        from staging.thingisbn s
        left join integration.work_isbn i on (s.isbn = i.isbn10 and s.work_uid = i.work_uid)
        where i.isbn10 is null;
        """,
        named_params={'logical_name': 'librarything'}))

    try:
        batch.execute_batch()
        if truncate_stage:
            elt.truncate_table("staging.thingisbn", True)
    except elt.EltError, er:
        print str(er)





def batch_loading_reviews():
    """
        inserts to do: reviewer, book, review, ..truncate staging
    :return:
    """
    pass



def insert_into_user():
    # ok since now() always return timestamp as of begining of transaction
    sql = \
        """
        insert into integration.user(id, site_id, username, load_audit_id, create_dts)
        select
            integration.derive_userid(r.username, r.site_logical_name) as userid
            , s.id
            , r.username
            , %(audit_id)s
            , now()
        from staging.review r
        join integration.site s on (r.site_logical_name = s.logical_name)
        except
        select
            id
            , site_id
            , username
            , %(audit_id)s
            , now()
        from integration.user;
        """


def insert_into_book():
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


def insert_into_review():
    sql = \
        """
        insert into integration.review(book_id, reviewer_id, review_date, rating_code, create_dts, load_audit_id)
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


def insert_into_book_site_review():
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


def insert_into_review_rejected():
    sql = \
        """
        insert into staging.review_rejected
        select *
        from staging.review r
        left join integration.book b on (r.derived_book_id = b.id)
        where b.id is null;
        """


def get_isbn_not_yet_associated():
    sql = \
        """
        select isbn13, isbn10
        from integration.isbn i
        left join integration.isbn_sameas s on i.isbn13 = s.isbn13_same
        where s.isbn13 is null;
        limit 1
        """

