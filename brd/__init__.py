# -*- coding: utf-8 -*-
from datetime import datetime

import fnmatch
import os
from os.path import isfile, join
import brd.elt as elt
import config

__author__ = 'mouellet'



def get_period_text(begin_period, end_period):
    """
    :return: 'd-m-yyyy_d-m-yyyy' from the specified begin/end_priod date
    or 'd-m-yyyy' when end_period is None
    """
    b = str(begin_period.day) + '-' + str(begin_period.month) + '-' + str(begin_period.year)
    if end_period is None:
        return b
    else:
        e = str(end_period.day) + '-' + str(end_period.month) + '-' + str(end_period.year)
        return b + '_' + e


def resolve_date_text(onedate, fmt='%d-%m-%Y'):
    if onedate is None:
        return None
    else:
        return datetime.strptime(onedate, fmt)


def resolve_period_text(period_text):
    """
    :param period_text: 'd-m-yyyy_d-m-yyyy'
    :return: (begin_period, end_period)
    """
    bp = period_text[0:period_text.index('_')]
    ep = period_text[period_text.index('_') + 1:]
    begin = resolve_date_text(bp)
    end = resolve_date_text(ep)
    return (begin, end)


def get_all_files(repdir, pattern, recursively):
    """
    Get all files under repdir and sub-dir (recursively or not).
    :return: list of files with matching pattern
    """
    if not os.path.lexists(repdir):
        raise EnvironmentError("Invalid directory defined :'" + repdir + "'")

    files = []
    if recursively:
        for root, dirnames, filenames in os.walk(repdir):
            for filename in fnmatch.filter(filenames, pattern):
                files.append(join(root, filename))
    else:
        allfiles = [f for f in os.listdir(repdir) if isfile(join(repdir, f))]
        for filename in fnmatch.filter(allfiles, pattern):
            files.append(join(repdir, filename))

    return files


def get_column_headers(file_with_header):
    """
    Read the column headers from specified text file
    (by default scrapy exports fields in any order, i.e. dict-like)
    :param file_with_header:
    :return: string of column headers with correct order
    """
    with open(file_with_header, 'r') as f:
        h = f.readline().split("|")
    return ",".join(h).strip()




# trigger reference data load
def load_static_ref():
    nb = elt.get_connection().fetch_one('select count(1) from integration.language')[0]
    if nb == 0:
        lang_ref_file = join(config.REF_DATA_DIR, 'Iso_639_and_Marc_code - ISO-639-2_utf-8.tsv')
        fields = 'code3,code3_term,code2,code,english_iso_name,english_name,french_iso_name,french_name'
        with open(lang_ref_file, 'r') as f:
            elt.copy_into_table('integration.language', fields, f, delim='\t')
        elt.get_connection().commit()

load_static_ref()


# caching langugage lookup
lang_cache = {}

def get_marc_code(input, capital=False):
    """
    input can be either alpha-2, alpha-3 code, english or french name
    capitalized or not.
    :param input: in UTF-8 encoding, otherwise the SQL comparison will fail (PS using UTF-8 encoding)
    :return: marc_code found or None
    """
    uinput = input.strip().upper()
    if uinput in lang_cache:
        marc_code = lang_cache[uinput]
    else:
        select = \
            """
            select input, code from
                (select upper(code2) as input, upper(code) as code from integration.language
                union select upper(code3), upper(code) from integration.language
                union select upper(english_name), upper(code) from integration.language
                union select upper(french_name), upper(code)  from integration.language) as foo
            where input = (%s)
            """
        ret = elt.get_connection().fetch_one(select, (uinput,))
        if ret:
            marc_code = ret[1]
        else:
            marc_code = None
    lang_cache[uinput] = marc_code
    if capital:
        return marc_code
    else:
        return marc_code.lower()


def convert_to_isbn13(isbn10):
    """
    Convert Isbn10 to Isbn13, or when it has 13 digits return as-is
    >>> convert_to_isbn13('1-56619-909-3')
    '9781566199094'
    >>> convert_to_isbn13(u'2868890067')
    '9782868890061'
    >>> convert_to_isbn13(2123456802)
    '9782123456803'
    >>> convert_to_isbn13('817450494X')
    '9788174504944'
    """
    isbn10_txt = str(isbn10)
    isbn10_txt = isbn10_txt.replace('-', '')

    if len(isbn10_txt) == 13:
        return isbn10_txt
    elif len(isbn10_txt) != 10:
        raise ValueError("Isbn10 '%s' has not 10 digits" % isbn10_txt)

    isbn13_no_checkdigit = '978' + isbn10_txt[:-1]
    s = 0
    mult = [1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3]
    for i in xrange(12):
        s += int(isbn13_no_checkdigit[i]) * mult[i]
    check = 10 - (s % 10)
    if check == 10:
        check = 0

    return isbn13_no_checkdigit + str(check)

