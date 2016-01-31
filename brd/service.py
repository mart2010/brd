# -*- coding: utf-8 -*-
import shutil

import brd
import brd.elt as elt
from brd.elt import BatchProcessor, Step
import brd.config as config
import os
import datetime
from brd.scrapy import SpiderProcessor

__author__ = 'mouellet'


def treat_loaded_file(processed_file, remove, archive_dir):
    if remove:
        os.remove(processed_file)
        return None
    else:
        shutil.move(processed_file, archive_dir)
        return archive_dir


# delay period before review can be harvested
elapse_days = 5


def get_end_period():
    """
    Rules to avoid Harvesting too recent reviews
    :return:
    """
    return datetime.date.today() - datetime.timedelta(days=elapse_days)


begin_default_date = '1-1-1900'


def get_default_begin_date():
    # default begin_date used for initial loading
    return brd.resolve_date_text(begin_default_date)


def _fetch_work(site_logical_name, harvested, nb_work):
    """
    Fetch info related to nb_work work-ids either harvested or not
    :return list of dict {'work-ori-id': idXXX, 'last_harvest_date': dateX, 'nb_in_db': {'ENG': 12, 'FRE': 2, ..}}
    """
    def _construct_dic(list_tuples):
        res = []
        if list_tuples is None or len(list_tuples) == 0:
            return res

        for row in list_tuples:
            dic = {'work-ori-id': row[0]}
            if len(row) > 1:
                dic['last_harvest_date'] = row[1]
                if row[2] and len(row[2]) > 0:
                    sub_dic = {}
                    for i in xrange(len(row[2])):
                        sub_dic[row[2][i]] = row[3][i]
                    dic['nb_in_db'] = sub_dic
                else:
                    dic['nb_in_db'] = None
            res.append(dic)
        return res

    # harvested=True (incremental) implies using the least recent harvested works
    if harvested:
        sql = \
            """
            select m.work_ori_id, m.last_harvest_date,
                    array_agg(l.lang_code), array_agg(l.nb_review)
            from integration.work_site_mapping m
            join integration.site s on (s.id = m.site_id)
            left join integration.reviews_persisted_lookup l
                           on (l.work_uid = m.ref_uid and l.logical_name = %(name)s)
            where
            s.logical_name = %(name)s
            and m.last_harvest_date IS NOT NULL
            group by 1,2
            order by 2 asc
            limit %(nb)s
            """
    # harvested=False (initial) implies listing the first n work never harvested
    else:
        sql = \
            """
            select m.work_ori_id
            from integration.work_site_mapping m
            join integration.site s on (s.id = m.site_id)
            where
            s.logical_name = %(name)s
            and m.last_harvest_date IS NULL
            limit %(nb)s
            """
    list_of_wids = elt.get_ro_connection().fetch_all(sql, {'nb': nb_work, 'name': site_logical_name})
    return _construct_dic(list_of_wids)


def get_dump_filename(spidername, begin, end):
    pre = config.REVIEW_PREFIX + spidername + "(audit-id)"   # special placeholder for audit-id
    post = "_" + brd.get_period_text(begin, end) + config.REVIEW_EXT
    return pre + post


def harvest_review_and_load(site_logical_name, nb_work=10):
    """
    Harvest nb_work reviews (initial) for works never harvested.  If all works are harvested,
    then harvest reviews incrementally based on how many are missing in DB (and before end_period)
    (incremental harvest nb of reviews = #inSite - #inDB).

    As a rule we load all reviews in staging after harvesting them, so we can update
    work's last_harvest_date in work_site_mapping.
    :param nb_work:
    :param site_logical_name:
    :return:
    """

    # Try processing Work never harvested (initial) !
    workids_to_harvest = _fetch_work(site_logical_name, harvested=False, nb_work=nb_work)
    if len(workids_to_harvest) >= 1:
        audit_id, dump_filename = _harvest_reviews(site_logical_name, workids_to_harvest, initial=True)
    # Otherwise, process Work already harvested (incremental) !
    else:
        workids_to_harvest = _fetch_work(site_logical_name, harvested=True, nb_work=nb_work)
        audit_id, dump_filename = _harvest_reviews(site_logical_name, workids_to_harvest, initial=False)

    # stage file so we can update stat in work_site_mapping...
    # stage_audit_id, nb_rec = _bulkload_file(dump_filename, 'staging.review', archive_file=True)

    # update last_update_date based on harvest audit_id (records in staging.reviews not linked to bulk-load audit-id)
    # _update_harvest_date(audit_id)


def _update_harvest_date(harvest_audit_id):
    sql = \
        """
        update integration.work_site_mapping map
            set last_harvest_date = work_in_staging.end_period
        from (select integration.derive_other_work_id(r.book_uid, r.site_logical_name) as w_id
                , a.id as audit_id
                , max(a.period_end) as end_period
              from staging.review r
              join staging.load_audit a on (r.load_audit_id = a.id)
              where a.id = %(audit)s
              group by 1,2
             ) as work_in_staging
        where map.work_id = work_in_staging.w_id;
        """
    ret = elt.get_connection().execute(sql, {'audit': harvest_audit_id})
    print("Update work_site_mapping for %s works " % ret)
    return ret


def _harvest_reviews(site_logical_name, workids_list, initial):
    """
    :param site_logical_name:
    :param workids_list:
    :param initial:
    :return: (audit-id, dump_filepath)
    """
    if initial:
        begin_period = get_default_begin_date()
    else:
        begin_period = None
    end_period = get_end_period()
    dump_filepath = os.path.join(config.SCRAPED_OUTPUT_DIR,
                                 get_dump_filename(site_logical_name, begin_period, end_period))
    spider_process = SpiderProcessor(site_logical_name,
                                     dump_filepath=dump_filepath,
                                     begin_period=begin_period,
                                     end_period=end_period,
                                     reviews_order='desc',
                                     works_to_harvest=workids_list)

    audit_id, dump_filepath = spider_process.start_process()
    print("Harvest of %d works/review completed with spider %s (initial: %s, audit_id: %s, dump file: '%s')" \
          % (len(workids_list), site_logical_name, initial, audit_id, dump_filepath))
    return (audit_id, dump_filepath)


def bulkload_thingisbn(pattern="thingISBN_10*.csv", remove_file=False, truncate_staging=True):
    """
    Try to load one reference file: thingISBN*_d-m-yyyy.csv
    :param remove_file:
    :param truncate_staging:
    :return:
    """
    file_to_load = brd.get_all_files(config.REF_DATA_DIR, pattern, recursively=False)

    if len(file_to_load) == 1:
        file_to_load = file_to_load[0]
    else:
        raise elt.EltError("Expected ONE thingISBN file to load: %s" % str(file_to_load))

    file_date = file_to_load[file_to_load.rindex('_') + 1:file_to_load.rindex('.csv')]
    # thingISBN data is a one-time snapshot (i.e. period_begin = period_end)
    period_begin = brd.resolve_date_text(file_date)

    if truncate_staging:
        elt.truncate_table({'schema': 'staging', 'table': 'thingisbn'}, True)

    n = elt.bulkload_file(file_to_load, 'staging.thingisbn', "WORK_UID, ISBN_ORI, ISBN10, ISBN13, LOAD_AUDIT_ID", (period_begin, period_begin))
    if n[1] != -1:
        treat_loaded_file(file_to_load, remove_file, config.REF_ARCHIVE_DIR)


def bulkload_review_files(filepattern, period=None, remove_files=False, first_truncate_staging=False):
    """
    NOT USED ANYMORE (Could be useful later when batch loading file...must manage not to havest same work-id prior to that)
    Bulk loads Reviews*.dat files into staging DB. By default, load all files
    otherwise only the ones corresponding to period specified.
    Commit is done after each file loaded
    :param period: 'd-m-yyyy_d-m-yyyy'
    :return: (nb of files treated, nb of files with error)
    """
    if period:
        begin_period, end_period = brd.resolve_period_text(period)
    if first_truncate_staging:
        elt.truncate_table({'schema': 'staging', 'table': 'review'}, True)

    n_treated, n_error = 0, 0
    for datfile in get_all_files(config.SCRAPED_OUTPUT_DIR, filepattern, True):
        file_begin, file_end = brd.resolve_period_text(datfile[datfile.index('_') + 1: datfile.rindex(config.REVIEW_EXT)])
        if period is None or (file_begin >= begin_period and file_end <= end_period):
            n = elt.bulkload_file(datfile, 'staging.review', get_column_headers(datfile), (file_begin, file_end))
            if n[1] == -1:
                n_error += 1
            else:
                n_treated += 1
                archivefile = treat_loaded_file(datfile, remove_files, config.SCRAPED_ARCHIVE_DIR)
            print("Finished bulkloading review file '%s', file was archived to '%s'" % (datfile, archivefile))
    return (n_treated, n_error)


def _bulkload_file(filepath, schematable, archive_file=True):
    """
    Bulkload filepath with headers, assuming table columns are same as headers and
    period found in filename as '*_beginxxx_endyyy.ext' (where begin is optional)
    move to archive dir
    :param filepath:
    :param archive_file: move to archive otherwise delete file
    :return: (audit-id, #ofRec bulkloaded)
    """
    period_text = filepath[filepath.index('_') + 1: filepath.rindex(".")]
    file_begin, file_end = brd.resolve_period_text(period_text)
    audit_id, n = elt.bulkload_file(filepath, schematable, brd.get_column_headers(filepath), (file_begin, file_end))
    treat_loaded_file(filepath, False, config.SCRAPED_ARCHIVE_DIR)
    print("Bulkload of %s completed (audit-id: %s, #OfLines: %s" %(filepath, audit_id, n))
    return (audit_id, n)


def batch_loading_workisbn(truncate_stage=False):
    """
    :param truncate_stage:
    :return:
    """
    batch = BatchProcessor(batch_loading_workisbn.func_name, 'thingisbn')
    batch.add_step(Step(
        name="load_work",
        sql="""
        insert into integration.work(uid, load_audit_id, create_dts)
        select distinct s.work_uid, %(audit_id)s, now()
        from staging.thingisbn s
        left join integration.work w on s.work_uid = w.uid
        where w.uid is null;
        """))

    batch.add_step(Step(
        name="load_isbn",
        sql="""
        insert into integration.isbn(ean, isbn13, isbn10, load_audit_id, create_dts)
        select distinct cast(s.isbn13 as bigint), s.isbn13, s.isbn10, %(audit_id)s, now()
        from staging.thingisbn s
        left join integration.isbn i on cast(s.isbn13 as bigint) = i.ean
        where i.ean is null;
        """))

    # TODO: design for possible deletion of work_uid/isbn (relevant also for work and isbn table) ?
    batch.add_step(Step(
        name="load_work_isbn",
        sql="""
        insert into integration.work_isbn(ean, work_uid, source_site_id, load_audit_id, create_dts)
        select distinct cast(s.isbn13 as bigint), s.work_uid, (select id from integration.site where logical_name = 'librarything')
                , %(audit_id)s, now()
        from staging.thingisbn s
        left join integration.work_isbn i on (cast(s.isbn13 as bigint) = i.ean and s.work_uid = i.work_uid)
        where i.ean is null;
        """))

    # mapping is one-to-one, but still needed to manage work/review harvest planning
    # not hardcoding librarything (bit could be reused for other site update..)
    batch.add_step(Step(
        name="load_work_site_mapping",
        sql="""
        insert into integration.work_site_mapping(ref_uid, work_id, site_id, work_ori_id, load_audit_id, create_dts)
        select work.uid, integration.derive_other_work_id(work.uid::text,%(logical_name)s)
                , work.site_id, work.uid_text, %(audit_id)s, now()
        from
        (select uid, uid::text as uid_text, (select id from integration.site where logical_name = %(logical_name)s) as site_id
        from integration.work) as work
        left join integration.work_site_mapping m on (work.uid_text = m.work_ori_id and m.site_id = work.site_id)
        where m.work_ori_id IS NULL;
        """,
        named_params={'logical_name': 'librarything'}))

    if truncate_stage:
        batch.add_step(Step(
            name="truncate_stage_thingisbn",
            sql="truncate staging.thing.isbn",
            named_params={'schema': 'staging', 'table': 'thingisbn'}))

    try:
        batch.execute_batch()
    except elt.EltError, er:
        print str(er)


def batch_loading_reviews():
    """
        inserts to do: reviewer, book, review, ..truncate staging
    :return:
    """
    pass


def get_isbn_not_yet_associated():
    sql = \
        """
        select isbn13, isbn10
        from integration.isbn i
        left join integration.isbn_sameas s on i.isbn13 = s.isbn13_same
        where s.isbn13 is null;
        limit 1
        """
    return elt.get_ro_connection().fetch_all(sql)



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


