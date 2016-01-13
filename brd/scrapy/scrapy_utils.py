# -*- coding: utf-8 -*-

__author__ = 'mouellet'

import re


# TODO: eventually refactor these various utils fcts into fct-based modules

# maybe use locale, but this simple solution works when exact 'mois' name is used!
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

def convert_book_title_to_sform(title):
    r"""
    (NO LONGER USED, AS TRANSFORMATION NOT DONE DURING SCRAPING)
    Convert raw title found in websites to this form :
        'capitalized-title-with-space-replaced-by-dash'
    For doctest to work, I need to flag this text as raw (r)
    >>> convert_book_title_to_sform(" title blank with leading/trailing  ")
    'TITLE-BLANK-WITH-LEADING/TRAILING'
    >>> convert_book_title_to_sform(" Here's a good \"garden\", to convert!!?")
    'HERE\'S-A-GOOD-"GARDEN",-TO-CONVERT!!?'
    """
    ctrim = title.strip().upper()
    return compile_regex.sub("-", ctrim)


# should be moved to service
def fetch_nbreviews(spider_name):
    query = """select  book_uid
                      ,count(one_review) as nb_reviews
                from integration.reviews_persisted_lookup
                where logical_name = %s
                group by book_uid;
             """

    res = elt.get_ro_connection().fetch_all_inTransaction(query, (spider_name, ))

    nbreviews_stored = {}
    if res is not None:
        for row in res:
            nbreviews_stored[row[0]] = row[1]
    return nbreviews_stored

# should be moved to service
def fetch_book_titles(scraper_name):
    """
    Fetch titles scrapped by other spiders but not for 'scraper_name'.
    Useful for site scraping new reviews through search (ex. babelio where no global list exist)
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
            and c.book_id = b.book_id);
    """
    return elt.get_ro_connection().fetch_all_inTransaction(query, (scraper_name, ))


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
            raise ValueError("Expected %d elements, but got: '%s' with selector '%s', xpath '%s' " % (expected, val, selector, xpath))
    return val

