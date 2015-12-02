# -*- coding: utf-8 -*-
import fnmatch
import os

__author__ = 'mouellet'

import re
import brd.db.dbutils as dbutils
from datetime import datetime



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


def get_all_files(repdir, pattern):
    """
    Get all files under repdir and sub-dir recursively.
    :return: list of files with matching pattern
    """
    if not os.path.lexists(repdir):
        raise EnvironmentError("Invalid directory defined :'" + repdir + "'")

    files = []
    for root, dirnames, filenames in os.walk(repdir):
        for filename in fnmatch.filter(filenames, pattern):
            files.append(os.path.join(root, filename))
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