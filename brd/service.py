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


def fetch_workwithisbn_not_harvested(site_logical_name, nb_work):
    """
    Find work_uid and their isbns not yet harvested for site.
    Use master work (when applicable) to reduce the risk of linking
    reviews to duplicate work (unfortunately, this may still happen
    when work was harvested before linkage done by lt)
    :return:
    """
    sql = \
        """
        with ref as (
            select coalesce(same.master_uid, w.uid) as wid, array_agg(i.isbn13) as isbn_list
            from integration.work w
            left join integration.work_sameas same on (w.uid = same.work_uid)
            join integration.work_isbn wi on (wi.work_uid = w.uid)
            join integration.isbn i on (i.ean = wi.ean)
            group by 1
        )
        select ref.wid as work_uid, ref.isbn_list as isbns
        from ref
        left join
            (select ref_uid, last_harvest_dts
             from integration.work_site_mapping m
             join integration.site s on (m.site_id = s.id and s.logical_name = %(name)s)
            ) as mapped on (mapped.ref_uid = ref.wid)
        where mapped.last_harvest_dts IS NULL
        limit %(nb)s
        """
    return elt.get_ro_connection().fetch_all(sql, {'nb': nb_work, 'name': site_logical_name}, as_dict=True)


def fetch_ltwids_not_harvested(nb_work):
    sql = \
        """
        select work_ori_id as work_uid
        from integration.work
        where
        last_harvest_dts IS NULL
        limit %(nb)s
        """
    return elt.get_ro_connection().fetch_all(sql, {'nb': nb_work})


def fetch_ltwids_stat_harvested(nb_work):
    sql = \
        """
        with cnt_per_lang as (
            select w.uid, w.last_harvest_dts, r.review_lang, count(1) as cnt
            from integration.work w
            left join integration.review r on (w.uid = r.work_uid)
            where
            w.last_harvest_dts IS NOT NULL
            and r.site_id = (select id from integration.site where logical_name = 'librarything')
            group by 1,2,3
        )
        select uid as work_uid, last_harvest_dts,
                array_agg(review_lang) as langs, array_agg(cnt) as cnts
        from cnt_per_lang
        group by 1,2
        order by 2 asc
        limit %(nb)s
        """
    return elt.get_ro_connection().fetch_all(sql, {'nb': nb_work}, as_dict=True)



def construct_dic(list_dic):
    """
    Construct the following list of dictionary (only work-id keyword mandatory) from tuple list


    :param list_dic: [{'work_uid':, 'last_harvest_dts':, 'langs':[], 'cnts':[], 'isbns':[] } ]
    :return: [  {'work_uid': w1,
        'last_harvest_dts': d1,
        'nb_in_db': {'ENG': 12, 'FRE': 2, ..},
        'isbns': [ix,iy,..]
        }, ...]
    """
    res = []
    if list_dic is None or len(list_dic) == 0:
        return res
    for row in list_dic:
        dic = {'work_uid': row['work_uid']}
        if len(row) > 1:
            dic['last_harvest_dts'] = row.get('last_harvest_dts')
            dic['isbns'] = row.get('isbns')
            if row.get('nb_in_db') and len(row['nb_in_db']) > 0:
                sub_dic = {}
                for i in xrange(len(row['nb_in_db'])):
                    sub_dic[row['nb_in_db'][i]] = row['cnts'][i]
                dic['nb_in_db'] = sub_dic
            else:
                dic['nb_in_db'] = None
        res.append(dic)
    return res



def fetch_ltwork_list(nb_work):
    """
    Fetch list of work but first looking at the ones not yet harvested, and if none is found then
     fetch already harvested (with additional stat):
    :return list of dict {'work_uid': x, 'last_harvest_dts': y, 'nb_in_db': {'ENG': n1, 'FRE': n2, ..}}
    """

    list_of_wids = fetch_ltwids_not_harvested(nb_work)
    if list_of_wids is None or len(list_of_wids) == 0:
        list_of_wids = fetch_ltwids_stat_harvested(nb_work)
        print "All work harvested for 'librarything', will go fetch harvested work"
    return construct_dic(list_of_wids)


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










