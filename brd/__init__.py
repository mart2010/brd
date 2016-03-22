# -*- coding: utf-8 -*-
import logging
from datetime import datetime

import fnmatch
import os
from os.path import isfile, join
import config
import brd.taskref
import brd.elt

__author__ = 'mouellet'


logger = logging.getLogger(__name__)
# module-level loggers use config file (otherwise, they get created with default values)
# also done in Luigi.interface so probaby not needed when launching task from command-line
# TODO: this is done by luigi.. now impact the logging of my test (see what to do instead of commented ot)
#logging.config.fileConfig('logging.cfg', disable_existing_loggers=False)


def get_period_text(begin_period, end_period):
    """
    :param begin_period:
    :return: 'd-m-yyyy_d-m-yyyy' from the specified begin/end_priod date
    or 'd-m-yyyy' when begin_period is None
    """
    assert(end_period is not None)
    e = str(end_period.day) + '-' + str(end_period.month) + '-' + str(end_period.year)
    if begin_period is None:
        return e
    else:
        b = str(begin_period.day) + '-' + str(begin_period.month) + '-' + str(begin_period.year)
        return b + '_' + e


def resolve_date_text(onedate, fmt='%d-%m-%Y'):
    """
    :param onedate:
    :param fmt:
    :return: onedate in date format (not datetime)
    """
    if onedate is None:
        return None
    else:
        return datetime.strptime(onedate, fmt).date()


def resolve_period_text(period_text):
    """
    :param period_text: 'd-m-yyyy_d-m-yyyy' or 'd-m-yyyy'
    :return: (begin_period, end_period)
    """
    ind_ = period_text.find('_')
    if ind_ != 0:
        bp = period_text[0:ind_]
        ep = period_text[ind_ + 1:]
    else:
        bp = None
        ep = period_text
    begin = resolve_date_text(bp)
    end = resolve_date_text(ep)
    return (begin, end)


def get_all_files(repdir, pattern, recursively):
    """
    Get all files under repdir and sub-dir (recursively or not).
    :param repdir:
    :param pattern:
    :param recursively:
    :return:
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


def get_column_headers(file_with_header, separator):
    """
    Read the column headers from specified file
    (by default scrapy exports fields in any order, i.e. dict-like)
    :param file_with_header:
    :return: list of column header in same order as in file
    """

    f = file_with_header.open('r')
    h = f.readline().split(separator)
    f.close()
    return h



# caching language lookup
lang_cache = {}


def get_marc_code(input, capital=True):
    """
    input can be either alpha-2, alpha-3 code, full english or french name (capitalized or not)
    :param input: text in UTF-8 encoding (otherwise SQL comparison will fail as PS uses UTF-8 encoding)
    :return: marc_code found or None
    """
    if input is None:
        return None
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
        ret = brd.elt.get_ro_connection().fetch_one(select, (uinput,))
        if ret:
            marc_code = ret[1]
        else:
            marc_code = '---'

    lang_cache[uinput] = marc_code
    if capital:
        return marc_code
    else:
        return marc_code.lower()


def convert_to_isbn13(isbn10):
    """
    Convert Isbn10 to Isbn13
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

    if len(isbn10_txt) != 10:
        raise ValueError("Isbn10 '%s' must have 10 chars" % isbn10_txt)

    isbn13_no_checkdigit = '978' + isbn10_txt[:-1]
    s = 0
    mult = [1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3]
    for i in xrange(12):
        s += int(isbn13_no_checkdigit[i]) * mult[i]
    check = 10 - (s % 10)
    if check == 10:
        check = 0

    return isbn13_no_checkdigit + str(check)


import luigi

# trigger reference data load
def load_static_ref():

    # language stuff
    c = brd.elt.get_connection()
    nb = c.fetch_one('select count(1) from integration.language')[0]
    if nb == 0:
        lang_ref_file = join(config.REF_DATA_DIR, 'Iso_639_and_Marc_code - ISO-639-2_utf-8.tsv')
        fields = 'code3,code3_term,code2,code,english_iso_name,english_name,french_iso_name,french_name'
        with open(lang_ref_file, 'r') as f:
            n = c.copy_into_table('integration.language', fields, f, delim='\t')
        c.commit()
        logger.info("Loaded %d records into integration.language" % n)

    #reference stuff
    # nb = c.fetch_one('select count(1) from integration.work')[0]
    # if nb == 0:
    #     logging.info("Work reference data empty, must load it first!")
    #     work_ref = brd.taskref.BatchLoadWorkRef()
    #     sch = luigi.scheduler.CentralPlannerScheduler()
    #     w = luigi.worker.Worker(scheduler=sch)
    #     w.add(work_ref)
    #     w.run()

load_static_ref()


