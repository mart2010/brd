# -*- coding: utf-8 -*-

__author__ = 'mouellet'

import re
import brd.db.dbutils as dbutils
from datetime import datetime


# TODO: eventually refactor these various utils fcts into fct-based modules

# maybe use locale later, but I know this simple solution works for exact mois name used!
mois = {
    u"janvier": "01",
    u"février": "02",
    u"mars": "03",
    u"avril": "04",
    u"mai": "05",
    u"juin": "06",
    u"juillet": "07",
    u"août": "08",
    u"septembre": "09",
    u"octobre": "10",
    u"novembre": "11",
    u"décembre": "12"
}




compile_regex = re.compile(r"\s+")

def convert_to_sform(title):
    r"""
    Convert raw title found in websites and transform into a format used after for MD5 hashing
    For doctest to work, I need to flag this text as raw (r)
    >>> convert_to_sform(" title blank with leading/trailing  ")
    'TITLE-BLANK-WITH-LEADING/TRAILING'
    >>> convert_to_sform(" Here's a good \"garden\", to convert!!?")
    'HERE\'S-A-GOOD-"GARDEN",-TO-CONVERT!!?'
    """
    ctrim = title.strip().upper()
    return compile_regex.sub("-", ctrim)


def fetch_nbreviews(scraper_name):
    query = """select  book_uid
                      ,count(one_review) as nb_reviews
                from integration.reviews_persisted_lookup
                where logical_name = %s
                group by book_uid
             """

    res = dbutils.get_ro_connection().fetch_all_inTransaction(query, (scraper_name, ))

    nbreviews_stored = {}
    if res is not None:
        for row in res:
            nbreviews_stored[row[0]] = row[1]
    return nbreviews_stored


def fetch_book_titles(scraper_name):
    """
    Fetch titles scrapped by other spiders but not for 'scraper_name'.
    Useful for site scrapping new revews through search (ex. babelio where no global list exist)
    :param scraper_name:
    :return:
    """
    query = """
        select max(title_text) as title_not_scraped
        from integration.book_site_review b
        where b.book_id not exists
            (select c.*
            from integration.book_site_review c
            join integration.site s on (s.id = c.site_id)
            where
            s.logical_name = %s  --logical name of babelio
            and c.book_id = b.book_id)
    """
    return dbutils.get_ro_connection().fetch_all_inTransaction(query, (scraper_name, ))



def resolve_value(selector, xpath, expected=1):
    """
    Return element values extracted from selector using xpath.
    :return: list of values or one value when expected=1 and list has one element.
    """
    val = selector.xpath(xpath).extract()
    if val is None:
        raise ValueError("Value extracted from selector '%s' with xpath: %s is None" % (selector, xpath))
    if hasattr(val, '__iter__'):
        if expected == len(val):
            return val[0] if expected == 1 else val
        else:
            raise ValueError("Expected %d elements, instead got: '%s' using selector '%s' with xpath '%s' " % (expected, val, selector, xpath))
    return val


def get_period_text(begin_period, end_period):
    """
    :return: 'd-m-yyyy_d-m-yyyy' from the specified begin/end_priod date
    """
    b = str(begin_period.day) + '-' + str(begin_period.month) + '-' + str(begin_period.year)
    e = str(end_period.day) + '-' + str(end_period.month) + '-' + str(end_period.year)
    return b + '_' + e

def resolve_period_text(period_text):
    """
    :param period_text: 'd-m-yyyy_d-m-yyyy'
    :return: (begin_period, end_period)
    """
    bp = period_text[0:period_text.index('_')]
    ep = period_text[period_text.index('_') + 1:]
    begin = datetime.strptime(bp, '%d-%m-%Y')
    end = datetime.strptime(ep, '%d-%m-%Y')
    return (begin, end)