__author__ = 'mouellet'

import re


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


