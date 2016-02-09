# -*- coding: utf-8 -*-
__author__ = 'mouellet'


from xml.etree.ElementTree import iterparse
import brd
import brd.config as config
import os
import datetime
import urllib
import subprocess

url_test = 'file:///Users/mart/Google Drive/brd/data_static/thingISBN_10000.xml'


def download_convert_thingISBN(url="http://www.librarything.com/feeds/thingISBN.xml.gz",
                               max_nb_line=-1,
                               enable_auditing=False):
    """
    Download/uncompress a copy of thingISBN.xml.gz from lt (or locally), and convert into csv while adding
    log-audit_id metadata.

    To be done sparingly with respect to licensing agreement (the file is roughly 50M compressed).
    :return: uncompressed filepath with date of download as suffix: "../thingISBN_1-31-2015.xml
    Can be tested using 'http://www.librarything.com/feeds/thingISBN_small.xml'
    """
    now = datetime.datetime.now()
    if enable_auditing:
        step = "Process file: " + url
        audit_id = brd.elt.insert_auditing(job=download_convert_thingISBN.__name__,
                                          step=step,
                                          begin=now,
                                          end=now,
                                          start_dts=now)
    else:
        audit_id = -1

    target_file = url[url.rindex('/') + 1:]
    if url.startswith('http'):
        local_filepath = download_file(url, config.REF_DATA_DIR, target_file)
    elif url.startswith('file'):
        local_filepath = url[7:]
    else:
        raise Exception('Url %s not valid' % url)

    local_filepath = uncompress_file(local_filepath)
    # add current date suffix to file
    suffix = "_" + brd.get_period_text(now, None) + ".xml"
    file_with_suffix = local_filepath[0:local_filepath.rindex('.xml')] + suffix
    os.rename(local_filepath, file_with_suffix)

    # convert to csv
    n_line = convert_to_csv_and_filter(file_with_suffix, audit_id, max_nb_line)
    if enable_auditing:
        brd.elt.update_auditing(commit=True,
                               rows=n_line,
                               status="Completed",
                               elapse_sec=(datetime.datetime.now()-now).seconds,
                               id=audit_id)


def download_file(url, target_dir, target_filename):
    download_f = os.path.join(target_dir, target_filename)
    urllib.urlretrieve(url, download_f)
    return download_f


def uncompress_file(filepath):
    gz = filepath.find('.gz')
    if gz != -1:
        cmd = []
        cmd.append('gzip')
        cmd.append('-d')
        cmd.append(filepath)
        # execute gzip (check exit code and raise CalledProcessError if code != 0)
        subprocess.check_call(cmd)
        return filepath[0:gz]
    else:
        return filepath


def convert_to_csv_and_filter(xml_pathfile, audit_id, max_nb_line=-1):
    """, ,
    Read xml file, filter isbn with attribute uncertain="true" and add
    associated work-id, isbn13, isbn10 fields.  Convert output to csv with '|' separator.

    :param max_nbwork: max number of work_uid to convert
    :return: number of line written to file
    """
    def generate_work_isbns(infile):
        for event, element in iterparse(infile):
            if event == 'end' and element.tag == 'work':
                for i in element.getchildren():
                    if 'uncertain' in i.attrib:
                        pass
                    else:
                        if len(i.text) == 10:
                            isbn10 = i.text
                            isbn13 = brd.convert_to_isbn13(isbn10)
                        elif len(i.text) == 13:
                            isbn10 = ''
                            isbn13 = i.text
                        else:
                            ValueError('Invalid isbn %s' % i.text)
                        yield (element.get('workcode'), i.text, isbn10, isbn13)

    outputfile = xml_pathfile[0:xml_pathfile.rindex('.xml')] + ".csv"

    f = open(outputfile, 'w')
    n = 0
    for l in generate_work_isbns(xml_pathfile):
        n += 1
        if max_nb_line != -1 and n >= max_nb_line:
            break
        f.write("%s|%s|%s|%s|%s\n" % (l[0], l[1], l[2], l[3], str(audit_id)))
    f.close()
    return n

