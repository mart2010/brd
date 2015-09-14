# -*- coding: utf-8 -*-

__author__ = 'mouellet'

import re
import brd.db.dbutils as dbutils


# TODO: eventually refactor these garbage utils fcts into separate module

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
    >>> convert_title(" title blank with leading/trailing  ")
    'TITLE-BLANK-WITH-LEADING/TRAILING'
    >>> convert_title(" Here's a good \"garden\", to convert!!?")
    'HERE\'S-A-GOOD-"GARDEN",-TO-CONVERT!!?'
    """
    ctrim = title.strip().upper()
    return compile_regex.sub("-", ctrim)


def fetch_nbreviews_stored(scraper_name):
    query = """select book_uid, nb_reviews
                from integration.%s
             """
    # tablename cannot be set as parameter, TODO: make View generic and filter on specific site domain or id instead..
    tablename = scraper_name + "_lookup"
    squery = query % tablename

    res = dbutils.get_ro_connection().execute_transaction(squery)

    nbreviews_stored = {}
    if res is not None:
        for row in dbutils.get_ro_connection().execute_transaction(squery):
            nbreviews_stored[row[0]] = row[1]
    return nbreviews_stored



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


