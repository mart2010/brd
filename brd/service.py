# -*- coding: utf-8 -*-
import logging
import logging.config
import shutil

import brd
import brd.elt as elt
import brd.config as config
import os
import datetime
import langdetect
import langdetect.lang_detect_exception as lang_detect_exception


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




_factory = None

def detect_text_language(text, default='eng'):
    """
    Detect language based on langdetect module using a prior map from reviews sample.
    This classifier is not precise (type-I error) with small text (between short to mid size), so it
    returns a default language (except for the listed language).

    >>> detect_text_language(u"small")
    u'und'
    >>> detect_text_language(u"A great life and biography.")
    u'eng'
    >>> detect_text_language(u"อ่านเพลิน ๆ")
    u'tha'
    >>> detect_text_language(u"J'ai adoré ce livre")
    u'und'
    >>> detect_text_language(u'http://wp.me/p20PAS-ct')
    u'und'
    >>> detect_text_language(u'how do you not read a book')
    u'eng'
    >>> detect_text_language(u'Voici un suspens à son comble!!')
    u'fre'
    >>> detect_text_language(u'Ottimo come infarinatura.')
    u'und'

    """
    short_text = 5
    mid_text = 30

    def get_factory():
        global _factory
        if _factory is None:
            _factory = langdetect.DetectorFactory()
            _factory.load_profile(os.path.join(langdetect.__path__[0], "profiles"))
        return _factory

    undetermined = u'und'
    if not text:
        return None
    elif len(text) < short_text:
        return undetermined

    detector = get_factory().create()
    detector.set_prior_map(brd.config.prior_prob_map)
    detector.append(text)
    try:
        lang_detected = detector.detect()
    except lang_detect_exception.LangDetectException:
        return undetermined

    if lang_detected == detector.UNKNOWN_LANG:
        return undetermined

    if short_text <= len(text) < mid_text:
        # Classifier precision is good for following languages:
        # 'eng','ara','per','spa','por','tur','jpn','rus','ben','cze','kor','bul','heb','gre','tha','ukr','mal','urd','mac','tam'
        if lang_detected in ('ar', 'bn', 'bg', 'cs', 'en', 'el', 'he', 'ja', 'ko', 'mk', 'ml', 'fa', 'pt', 'ru', 'es', 'ta', 'th', 'tr', 'uk', 'ur'):
            return brd.get_marc_code(lang_detected, capital=False)
        else:
            return default
    else:
        return brd.get_marc_code(lang_detected, capital=False)


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


def fetch_workIds_not_harvested(site_logical_name, nb_work, lang_code='eng', orderby_pop=False):
    """
    Fetch work_refid/isbns not yet harvested (mapping not present)
    using a source work_info and isbn_info (for lang) and, optionally order by popularity (more popular first)

    For lt, return ids for work that had their reference harvested.

    :param nb_work: number of work-ids to fetch
    :param lang_code: filter isbn on lang_code (ex. to limit isbn for lang-specific site).
    (not used for lt)
    :return:
    """
    sql_other = \
        """
        with ref as (
            select  coalesce(same.master_refid, w.work_refid) as wid
                    , w.popularity
                    , array_agg(wi.ean) as isbn_list
            from integration.work_info w
            left join integration.work_sameas same on (w.work_refid = same.work_refid)
            join integration.work_isbn wi on (wi.work_refid = w.work_refid and wi.deletion_date IS NULL )
            join integration.isbn_info ii on (wi.ean = ii.ean and ii.lang_code = %(lang)s)
            group by 1,2
        )
        select ref.wid as work_refid, ref.isbn_list as isbns
        from ref
        left join
            (select work_refid, last_harvest_dts
             from integration.work_site_mapping m
             join integration.site s on (m.site_id = s.id and s.logical_name = %(name)s)
            ) as mapped on (mapped.work_refid = ref.wid)
        where mapped.last_harvest_dts IS NULL
        order by {order_by}
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
        order by {order_by}
        limit %(nb)s
        """

    if orderby_pop:
        order_by = 'popularity'
    else:
        order_by = '1'

    if site_logical_name == 'librarything':
        sql = sql_lt.format(order_by=order_by)
        return elt.get_ro_connection().fetch_all(sql, {'nb': nb_work}, as_dict=True)
    else:
        sql = sql_other.format(order_by=order_by)
        return elt.get_ro_connection().fetch_all(sql, {'nb': nb_work,
                                                       'name': site_logical_name,
                                                       'lang': lang_code}, as_dict=True)


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


if __name__ == '__main__':
    import doctest
    doctest.testmod()
