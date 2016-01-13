# -*- coding: utf-8 -*-
from datetime import datetime

import fnmatch
import os
from os.path import isfile, join

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


def resolve_onedate_text(onedate, fmt='%d-%m-%Y'):
    return datetime.strptime(onedate, fmt)


def resolve_period_text(period_text):
    """
    :param period_text: 'd-m-yyyy_d-m-yyyy'
    :return: (begin_period, end_period)
    """
    bp = period_text[0:period_text.index('_')]
    ep = period_text[period_text.index('_') + 1:]
    begin = resolve_onedate_text(bp)
    end = resolve_onedate_text(ep)
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
                files.append(os.path.join(root, filename))
    else:
        allfiles = [f for f in os.listdir(repdir) if isfile(join(repdir, f))]
        for filename in fnmatch.filter(allfiles, pattern):
            files.append(os.path.join(repdir, filename))

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



