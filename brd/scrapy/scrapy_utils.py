# -*- coding: utf-8 -*-
import re

__author__ = 'mouellet'


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


regex_space = re.compile(r"\s+")

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
    return regex_space.sub("-", ctrim)


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

