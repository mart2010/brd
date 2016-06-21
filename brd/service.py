# -*- coding: utf-8 -*-
import logging
import logging.config
import shutil

import brd
import brd.elt as elt
import brd.config as config
import os
import datetime
from langid.langid import LanguageIdentifier, model



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


# Review's language with shorter text is set as u'und'
MIN_DETECT_SIZE = 30
# Review's language detected with probability smaller is set as u'und'
MIN_DETECT_PROB = 0.70

_langidentifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)

def detect_text_language(text):
    """
    Detect language now based on langid (langdetect fails miserably).
    When classification is below probability_min or text is too short
    returns undetermined language

    >>> detect_text_language(u"review with text too small 30")
    u'und'
    >>> detect_text_language(u'Mixed language etrange et muy bieno super confusing')
    u'und'
    >>> detect_text_language(u"סיפור נפלא ממש. תרגום בסדר פלוס.")
    u'heb'
    >>> detect_text_language(u"J'ai adoré ce livre mais il était long")
    u'fre'
    >>> detect_text_language(u"CLASSIC BOOKS YOU CAN'T BELIEVE ANYONE WOULD EVER READ EXCEPT FOR SCHOOL OR TO LOOK SMART?")
    u'eng'
    """

    global MIN_DETECT_SIZE
    global MIN_DETECT_PROB

    if not text:
        return None
    elif len(text) < MIN_DETECT_SIZE:
        return u'und'

    # langid does not work well with all-capitalized text
    lang_prob = _langidentifier.classify(text.lower())

    if lang_prob[1] < MIN_DETECT_PROB:
        return u'und'
    else:
        return brd.get_marc_code(lang_prob[0], capital=False)


# hack to avoid issue with ever longer index-scan to process...
next_rev_id = 1

# simple batch-processor for correcting review language
# on n reviews having review_lang = NULL OR = 'und'
def batch_detect_language(n_reviews):
    src_sql = \
        """
        select id, review
        from integration.review
        where
        id >= %(next_id)s
        and review_lang IS NULL
        and review IS NOT NULL
        order by 1
        limit %(nb)s;
        """
    upd_sql = \
        """
        update integration.review as t set review_lang = s.lang
        from (values {values_src}
        ) as s(id, lang)
        where s.id = t.id;
        """
    global next_rev_id
    step_size = 1000

    for i in xrange(0, n_reviews, step_size):
        rows_src = elt.get_ro_connection().fetch_all(src_sql, params={'next_id': next_rev_id, 'nb': step_size}, in_trans=True)
        tgt_list = [(rev_t[0], detect_text_language(rev_t[1])) for rev_t in rows_src]

        values_src = str(tgt_list).strip("[]").replace("u'", "'").replace("L,", ",")
        update_sql = upd_sql.format(values_src=values_src)
        rowcount = elt .get_connection().execute_inTransaction(update_sql)

        next_rev_id = rows_src[-1][0]
        logger.info("Batch detect language processed %d reviews (last rowcount= %d, last id= %d )" % (i, rowcount, next_rev_id))



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
    # These two are just temp and should be removed...
    sql_temp_harvested_in_babelio_not_lt = \
        """
        select m.work_refid
        from integration.work_site_mapping m
        join integration.work w on (w.refid = m.work_refid and site_id = 4 and w.last_harvest_dts is null)
        order by {order_by}
        limit %(nb)s
        """
    sql_temp_harvested_in_babelio_not_gr = \
        """
        select ba.work_refid, array_agg(wi.ean) as isbns
        from integration.work_site_mapping ba
        left join integration.work_site_mapping gr on (ba.work_refid = gr.work_refid and ba.site_id = 4 and gr.site_id = 2)
        left join integration.work_sameas s on (ba.work_refid = s.work_refid)
        join integration.work_isbn wi on (ba.work_refid = wi.work_refid)
        where
        ba.site_id = 4
        and gr.work_refid is null
        and s.work_refid is null
        group by 1
        limit %(nb)s
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
        sql = sql_temp_harvested_in_babelio_not_lt.format(order_by=order_by)
        return elt.get_ro_connection().fetch_all(sql, {'nb': nb_work}, as_dict=True)
    else:
        sql = sql_temp_harvested_in_babelio_not_gr.format(order_by=order_by)
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
