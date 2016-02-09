# -*- coding: utf-8 -*-
import shutil

import brd
import brd.elt as elt
from brd.elt import EltStepStatus
import brd.config as config
import os
import datetime
from brd.scrapy import SpiderProcessor

__author__ = 'mouellet'


def treat_loaded_file(processed_filepath, remove, archive_dir):
    if remove:
        os.remove(processed_filepath)
        return None
    else:
        filename = os.path.basename(processed_filepath)
        archivefilepath = os.path.join(archive_dir, filename)
        shutil.move(processed_filepath, archivefilepath)
        return archivefilepath


# delay period before review can be harvested
elapse_days = 5



def fetch_work(site_logical_name, harvested, nb_work):
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
            select m.work_ori_id, m.last_harvest_dts,
                    array_agg(l.lang_code), array_agg(l.nb_review)
            from integration.work_site_mapping m
            join integration.site s on (s.id = m.site_id)
            left join integration.reviews_persisted_lookup l
                           on (l.work_uid = m.ref_uid and l.logical_name = %(name)s)
            where
            s.logical_name = %(name)s
            and m.last_harvest_dts IS NOT NULL
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
            and m.last_harvest_dts IS NULL
            limit %(nb)s
            """
    list_of_wids = elt.get_ro_connection().fetch_all(sql, {'nb': nb_work, 'name': site_logical_name})
    return _construct_dic(list_of_wids)


# def harvest_review_and_load(spidername, nb_work=10):
#     """
#     Harvest nb_work reviews (initial) for works never harvested.  If all works are harvested,
#     then harvest reviews incrementally based on how many are missing in DB (and before end_period)
#     (incremental harvest nb of reviews = #inSite - #inDB).
#
#     We load all reviews in staging, update work's last_harvest_date in work_site_mapping,
#     and proceed with the rest of the integration loads.
#     :param nb_work:
#     :param spidername:
#     :return:
#     """
#
#     def prepare_batch(batch):
#         batch = BatchProcessor(batchname)
#
#         bulkload = \
#             Step(name="Bulkload harvest Reviews",
#                 sql_or_callable=_bulkload_file,
#                 named_params={'filepath': dump_filepath,
#                               'schematable'})
#
#         update_mapping = \
#             Step(name="Update last-harvest-date in mapping",
#                 sql_or_callable=_update_harvest_date,
#                 named_params={'site_logical_name': spidername, 'end_period': "to be given",
#                               'harvested_work_ids': "to be given"})
#
#         integrate_review = \
#             Step(name="Integrate reviews",
#                 sql_or_callable=
#                 """
#                 insert into integration.review(book_id, reviewer_id, review_date, rating_code, create_dts, load_audit_id)
#                 select
#                     integration.derive_bookid( integration.get_sform(r.book_title),
#                                                r.book_lang,
#                                                integration.standardize_authorname(r.author_fname, r.author_lname) ) as bookid
#                     , integration.derive_reviewerid( r.username, r.site_logical_name ) as reviewerid
#                     , r.parsed_review_date
#                     , r.review_rating
#                     , now()
#                     , %(audit_id)s
#                 from staging.review r
#                 """,
#                  named_params={'filepath': dump_filepath})
#
#         integrate_user = \
#             Step(name="Integrate users",
#                 sql_or_callable=
#                 """
#                 insert into integration.user(id, site_id, username, load_audit_id, create_dts)
#                 select
#                     integration.derive_userid(r.username, r.site_logical_name) as userid
#                     , s.id
#                     , r.username
#                     , %(audit_id)s
#                     , now()
#                 from staging.review r
#                 join integration.site s on (r.site_logical_name = s.logical_name)
#                 except
#                 select
#                     id
#                     , site_id
#                     , username
#                     , %(audit_id)s
#                     , now()
#                 from integration.user;
#                 """)
#
#         batch.add_step(Step(
#
#     batch = Batch("Harvest works from %s" % spidername
#
#





def _bulkload_file(filepath, schematable, archive_file=True):
    """
    Bulkload filepath with headers, assuming table columns are same as headers and
    period found in filename as '*_beginxxx_endyyy.ext' (where begin is optional)
    move to archive dir
    :param filepath:
    :param archive_file: move to archive otherwise leave it
    :return: (audit-id, #ofRec bulkloaded) #ofOfRec = 0 when file was empty)
    """
    def file_is_empty(filep):
        return os.stat(filep).st_size == 0

    period_text = filepath[filepath.index('_') + 1: filepath.rindex(".")]
    file_begin, file_end = brd.resolve_period_text(period_text)

    if file_is_empty(filepath):
        n = 0
        audit_id = elt.insert_auditing(commit=True, job='Bulkload file', step=filepath, begin=file_begin, end=file_end,
                                       status='File empty is not loaded', start_dts=datetime.datetime.now())
    else:
        audit_id, n = elt.bulkload_file(filepath, schematable, brd.get_column_headers(filepath), (file_begin, file_end))

    if n != -1 and archive_file:
        treat_loaded_file(filepath, remove=False, archive_dir=config.SCRAPED_ARCHIVE_DIR)
    return (audit_id, n)


def load_work_ref():
    sql = \
        """
        insert into integration.work(uid, create_dts)
        select distinct s.work_uid, now()
        from staging.thingisbn s
        left join integration.work w on s.work_uid = w.uid
        where w.uid is null;
        """
    return elt.get_connection().execute_inTransaction(sql)


def load_isbn_ref():
    sql = \
        """
        insert into integration.isbn(ean, isbn13, isbn10, create_dts)
        select distinct cast(s.isbn13 as bigint), s.isbn13, s.isbn10, now()
        from staging.thingisbn s
        left join integration.isbn i on cast(s.isbn13 as bigint) = i.ean
        where i.ean is null;
        """
    return elt.get_connection().execute_inTransaction(sql)

# TODO: what about possible deletion of work_uid/isbn (relevant also for work and isbn table)
def load_work_isbn_ref():
    sql = \
        """
        insert into integration.work_isbn(ean, work_uid, source_site_id, create_dts)
        select distinct cast(s.isbn13 as bigint), s.work_uid, (select id from integration.site where logical_name = 'librarything')
                , now()
        from staging.thingisbn s
        left join integration.work_isbn i on (cast(s.isbn13 as bigint) = i.ean and s.work_uid = i.work_uid)
        where i.ean is null;
        """
    return elt.get_connection().execute_inTransaction(sql)


def load_work_site_mapping(site_logical_name='librarything'):
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
    return elt.get_connection().execute_inTransaction(sql, params={'logical_name': site_logical_name})



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





