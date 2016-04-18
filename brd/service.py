# -*- coding: utf-8 -*-
import logging
import logging.config
import shutil

import brd
import brd.elt as elt
import brd.config as config
import os
import datetime
__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The BRD Project"


logger = logging.getLogger(__name__)

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
    For lt, simply return the ids using work_info ids to avoid harvesting duplicated work
    (still possible if the work was merged after Reference data was harvested).
    :return:
    """
    sql_other = \
        """
        with ref as (
            select coalesce(same.master_refid, w.work_refid) as wid, array_agg(wi.ean) as isbn_list
            from integration.work_info w
            left join integration.work_sameas same on (w.work_refid = same.work_refid)
            join integration.work_isbn wi on (wi.work_refid = w.work_refid)
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
        -- here we could order by popularity
        order by 1
        limit %(nb)s
        """
    # select only ones with work_info harvested
    sql_lt = \
        """
        select wi.work_refid
        from integration.work_info wi
        inner join integration.work w on (wi.work_refid = w.refid)
        where
        w.last_harvest_dts IS NULL
        order by 1
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
        select w.refid, cast(w.last_harvest_dts as date) as last_harvest_date,
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
    Construct the following list of dictionary (only work-id keyword mandatory)
    from the list input

    :param list_dic: [{'work_refid':, 'last_harvest_date':, 'langs':[], 'cnts':[], 'isbns':[]}..]
    :return: [  {'work_refid': w1,
        'last_harvest_date': d1,
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
            dic['last_harvest_date'] = row.get('last_harvest_date')
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


