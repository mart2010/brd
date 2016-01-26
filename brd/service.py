# -*- coding: utf-8 -*-
import shutil

import brd
from brd import get_all_files, get_column_headers
import brd.elt as elt
from brd.elt import BatchProcessor, Step
import brd.config as config
import os
import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

__author__ = 'mouellet'




def treat_loaded_file(processed_file, remove, archive_dir):
    if remove:
        os.remove(processed_file)
    else:
        shutil.move(processed_file, archive_dir)


# default min_date used for initial loading mode
init_begin_date = '1-1-1900'
# delay period before review can be harvested
elapse_days = 5


def get_end_period():
    """
    Rules to avoid Harvest reviews that are too recent
    :return:
    """
    return datetime.date.today() - datetime.timedelta(days=elapse_days)


def fetch_work_ids(site_logicalname, never_harvested=False, n_limit=None):
    """
    Fetch work-ids from specified site, that have already (or never) been harvested
    :return list of dic {'work-site-id1': idXXX, 'last_harvest_date': dateX, 'nb_in_db': {'ENG': 12, 'FRE': 2, ..}}
    """
    param = {"logical_name": site_logicalname}

    select = \
        """
        select m.work_ori_id, m.last_harvest_date %s
        from integration.work_site_mapping m
        join integration.site s on (s.id = m.site_id and s.name = %(sitelogical_name)s)
        """
    if never_harvested:
        sql = select % "" + " where m.last_harvest_date IS NULL "
    else:
        select = select % ", array_agg(l.lang_code), array_agg(l.nb_review)"
        join = \
            """
            left join integration.reviews_persisted_lookup l
                       on (l.work_uid = m.ref_uid and l.logical_name = %(sitelogical_name)s)
            where m.last_harvest_date IS NOT NULL
            group by 1, 2
            """
        sql = select + join

    if n_limit:
        sql = sql + " limit %(nb)s"
        param['nb'] = n_limit

    result = elt.get_ro_connection().fetch_all(sql, param)

    # construct list
    res = []
    for row in result:
        dic = {'work-site-id': row[0], 'last_harvest_date': row[1]}

        if row[2] and len(row[2]) > 0:
            sub_dic = {}
            for i in xrange(len(row[2])):
                sub_dic[row[2][i]] = row[3][i]
            dic['nb_in_db'] = sub_dic
        else:
            dic['nb_in_db'] = None
        res.append(dic)
    return res


def get_dump_filename(spidername, begin, end, audit_id):
    pre = config.REVIEW_PREFIX + spidername + "(%s)" % audit_id
    post = "_" + brd.get_period_text(begin, end) + config.REVIEW_EXT
    return pre + post


def initial_harvest_reviews(spidername, nb_work_fetch):

    begin_period = init_begin_date
    end_period = get_end_period()
    reviews_db_stats = fetch_work_ids(spidername, never_harvested=True, n_limit=nb_work_fetch)
    reviews_order = 'asc'

    # audit record has the correct period
    step = "preparing dump of '%s'" % spidername
    audit_id = brd.elt.insert_auditing(job=initial_harvest_reviews.__name__,
                                       step=step,
                                       begin=begin_period,
                                       end=end_period,
                                       start_dts=datetime.datetime.now())

    dump_filepath = os.path.join(config.REF_DATA_DIR, \
                                 get_dump_filename(spidername, begin_period, end_period, audit_id))

    # spider instantiation delegated to a builder-by-name (based name)

    # re-update step name (circular dependency between filename/audit-id)
    step = "Dump file: " + dump_filepath
    brd.elt.get_connection().execute("update staging.load_audit set step_name=%s where id=%s", (step, audit_id))

    # use settings.py defined under scrapy package
    process = CrawlerProcess(get_project_settings())

    process.crawl(spidername,
                  begin_period=begin_period,
                  end_period=end_period,
                  dump_filepath=dump_filepath,
                  work_to_harvest=reviews_db_stats,
                  reviews_order=reviews_order)
    # blocks here until crawling is finished
    process.start()




    brd.elt.update_auditing(commit=True,
                                rows=self.counter,
                                status="Completed",
                                finish_dts=datetime.datetime.now(),
                                id=self.audit[spider.name])

    # update stat in work_site_mapping.... eventhough only harvest files were generated
    # this must be based on the set of woek-ids obtained (fetch..) and not based on reviews harvested (don't want to keep harvesting unpopular work with no reviews!)



def incremental_harvest():
    pass


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
    # thingISBN data is a one-time snapshot (i.e. period_begin = period_end)
    period_begin  = brd.resolve_date_text(file_date)

    if truncate_staging:
        elt.truncate_table({'schema': 'staging', 'table': 'thingisbn'}, True)

    n = elt.bulkload_file(file_to_load, 'staging.thingisbn', "WORK_UID, ISBN, LOAD_AUDIT_ID", (period_begin, period_begin))
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
    if period:
        begin_period, end_period  = brd.resolve_period_text(period)
    if truncate_staging:
        elt.truncate_table({'schema': 'staging', 'table': 'review'}, True)

    pattern = config.REVIEW_PREFIX + "*"
    n_treated, n_error = 0, 0
    for datfile in get_all_files(config.SCRAPED_OUTPUT_DIR, pattern, True):
        file_begin, file_end = brd.resolve_period_text(datfile[datfile.index('_') + 1: datfile.rindex(config.REVIEW_EXT)])

        if period is None or (file_begin >= begin_period and file_end <= end_period):
            n = elt.bulkload_file(datfile, 'staging.review', get_column_headers(datfile), (file_begin, file_end))
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
        insert into integration.isbn(isbn10, load_audit_id, create_dts)
        select distinct s.isbn, %(audit_id)s, now()
        from staging.thingisbn s
        left join integration.isbn i on s.isbn = i.isbn10
        where i.isbn10 is null;
        """))

    # TODO: design for possible deletion of work_uid/isbn (relevant also for work and isbn table) ?
    # not hardcoding librarything for layer reuse..
    batch.add_step(Step(
        name="load_work_isbn",
        sql="""
        insert into integration.work_isbn(isbn10, work_uid, source_site_id, load_audit_id, create_dts)
        select distinct s.isbn, s.work_uid, (select id from integration.site where logical_name = 'librarything')
                , %(audit_id)s, now()
        from staging.thingisbn s
        left join integration.work_isbn i on (s.isbn = i.isbn10 and s.work_uid = i.work_uid)
        where i.isbn10 is null;
        """))

    # mapping is one-to-one, but still needed to manage work/review lifecycle
    batch.add_step(Step(
        name="load_work_site_mapping",
        sql="""
        insert into integration.work_site_mapping(ref_uid, work_id, site_id, work_ori_id, state, load_audit_id, create_dts)
        select uid, cast(md5(uid::text) as uuid), (select id from integration.site where logical_name = 'librarything')
            , uid::text, %(state)s, %(audit_id)s, now()
        from integration.work
        """,
        named_params={'state': WorkReviewState.MAPPED}))

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
    return elt.get_ro_connection().fetch_all(sql)

