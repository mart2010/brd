# -*- coding: utf-8 -*-
import logging
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


def fetch_workIds_no_info(nb_work):
    sql = \
        """
        select w.refid
        from integration.work w
        left join integration.work_info wi on (w.refid = wi.work_refid)
        left join integration.work_sameas s on (w.refid = s.work_refid)
        where wi.work_refid IS NULL
        and s.work_refid IS NULL
        limit %(nb)s
        """
    return elt.get_ro_connection().fetch_all(sql, {'nb': nb_work}, as_dict=True)


def fetch_workIds_not_harvested(site_logical_name, nb_work):
    """
    Find work_refid and their isbns not yet harvested for site.
    For lt, simply return the ids...  but using work_info ids to reduce the likelihood
    of harvesting duplicated work.

    :return:
    """
    sql_other = \
        """
        with ref as (
            select coalesce(same.master_refid, w.refid) as wid, array_agg(i.isbn10) as isbn_list
            from integration.work w
            left join integration.work_sameas same on (w.refid = same.work_refid)
            join integration.work_isbn wi on (wi.work_refid = w.refid)
            join integration.isbn i on (i.ean = wi.ean)
            group by 1
        )
        select ref.wid as work_refid, ref.isbn_list as isbns
        from ref
        left join
            (select work_refid, last_harvest_dts
             from integration.work_site_mapping m
             join integration.site s on (m.site_id = s.id and s.logical_name = %(name)s)
            ) as mapped on (mapped.work_refid = ref.wid)
        where mapped.last_harvest_dts IS NULL
        limit %(nb)s
        """
    # select only the ones with work_info harvested
    sql_lt = \
        """
        select wi.work_refid
        from integration.work_info wi
        where
        inner join integration.work w on (wi.work_refid = w.refid)
        w.last_harvest_dts IS NULL
        limit %(nb)s
        """
    if site_logical_name == 'librarything':
        return elt.get_ro_connection().fetch_all(sql_lt, {'nb': nb_work}, as_dict=True)
    else:
        return elt.get_ro_connection().fetch_all(sql_other, {'nb': nb_work, 'name': site_logical_name}, as_dict=True)


def fetch_ltwids_stat_harvested(nb_work):
    sql = \
        """
        with cnt_per_lang as (
            select r.work_refid as work_refid, r.review_lang, count(1)
            from integration.review r
            join integration.site s on (s.id = r.site_id and s.logical_name = 'librarything')
            group by 1,2
        )
        select w.refid, w.last_harvest_dts,
                array_agg(review_lang) as langs, array_agg(cnt) as cnts
        from work w
        left join cnt_per_lang c on (w.refid = c.work_refid)
        where w.last_harvest_dts IS NOT NULL
        group by 1,2
        order by 2 asc
        limit %(nb)s
        """
    return elt.get_ro_connection().fetch_all(sql, {'nb': nb_work}, as_dict=True)


def construct_dic(list_dic):
    """
    Construct the following list of dictionary (only work-id keyword mandatory) from tuple list

    :param list_dic: [{'work_refid':, 'last_harvest_dts':, 'langs':[], 'cnts':[], 'isbns':[] } ]
    :return: [  {'work_refid': w1,
        'last_harvest_dts': d1,
        'nb_in_db': {'ENG': 12, 'FRE': 2, ..},
        'isbns': [ix,iy,..]
        }, ...]
    """
    res = []
    if list_dic is None or len(list_dic) == 0:
        return res
    for row in list_dic:
        dic = {'work_refid': row['work_refid']}
        if len(row) > 1:
            dic['last_harvest_dts'] = row.get('last_harvest_dts')
            dic['isbns'] = row.get('isbns')
            if row.get('nb_in_db') and len(row['nb_in_db']) > 0:
                sub_dic = {}
                for i in xrange(len(row['nb_in_db'])):
                    sub_dic[row['langs'][i]] = row['cnts'][i]
                dic['nb_in_db'] = sub_dic
            else:
                dic['nb_in_db'] = None
        res.append(dic)
    return res



def fetch_ltwork_list(nb_work):
    """
    Fetch list of work but first looking at the ones not yet harvested, and if none is found then
     fetch already harvested (with additional stat):
    :return list of dict {'work_refid': x, 'last_harvest_dts': y, 'nb_in_db': {'ENG': n1, 'FRE': n2, ..}}
    """

    list_of_wids = fetch_ltwids_not_harvested(nb_work)
    if list_of_wids is None or len(list_of_wids) == 0:
        list_of_wids = fetch_ltwids_stat_harvested(nb_work)
        logging.info("All work harvested for 'librarything', will go fetch harvested work")
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










