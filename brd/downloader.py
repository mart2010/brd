# -*- coding: utf-8 -*-
__author__ = 'mouellet'


from xml.etree.ElementTree import iterparse
import brd.elt
import brd.config as config
import os
import datetime
import urllib
import subprocess



def launch_download_convert_thingISBN(url="http://www.librarything.com/feeds/thingISBN.xml.gz", max_nb_line=-1):
    """
    Download/uncompress a copy of thingISBN.xml.gz from lt, and convert into csv while adding
    log-audit_id metadata.

    To be done sparingly with respect to licensing agreement (the file is roughly 50M compressed).
    :return: uncompressed filepath with date of download as suffix: "../thingISBN_1-31-2015.xml
    Can be tested using 'http://www.librarything.com/feeds/thingISBN_small.xml'
    """
    start_date = datetime.datetime.now()
    step = "Download file: " + url
    audit_id = brd.elt.insert_auditing(job=launch_download_convert_thingISBN.__name__,
                                      step=step,
                                      begin=start_date,
                                      end=start_date,
                                      start_dts=start_date)

    target_file = url[url.rindex('/') + 1:]
    uncompress_file = download_file_and_uncompress(url, config.REF_DATA_DIR, target_file)

    suffix = "_" + brd.get_period_text(start_date, None) + ".xml"
    file_with_suffix = uncompress_file[0:uncompress_file.rindex('.xml')] + suffix
    os.rename(uncompress_file, file_with_suffix)

    # convert to csv
    n_line = convert_file_to_csv(file_with_suffix, audit_id, max_nb_line)
    brd.elt.update_auditing(commit=True,
                           rows=n_line,
                           status="Completed",
                           finish_dts=datetime.datetime.now(),
                           id=audit_id)



def download_file_and_uncompress(url, target_dir, target_filename):

    download_file = os.path.join(target_dir, target_filename)
    urllib.urlretrieve(url, download_file)
    gz = download_file.find('.gz')
    if gz != -1:
        cmd = []
        cmd.append('gzip')
        cmd.append('-d')
        cmd.append(download_file)
        # execute gzip (check exit code and raise CalledProcessError if code != 0)
        subprocess.check_call(cmd)
        return download_file[0:gz]
    else:
        return download_file



def convert_file_to_csv(xml_pathfile, audit_id, max_nb_line=-1):
    """
    Tranform xml_pathfile from xml to csv (with '|' separator)
    :param max_nbwork: max number of work_uid to convert
    :return: number of line written to file
    """
    def generate_work_isbns(infile):
        for event, element in iterparse(infile):
            if event == 'end' and element.tag == 'work':
                # TODO:  deal with the work having attribute uncertain="true"  ..maybe ignore them
                for w in element.getchildren():
                    yield (element.get('workcode'), w.text)

    outputfile = xml_pathfile[0:xml_pathfile.rindex('.xml')] + ".csv"

    f = open(outputfile, 'w')
    n = 0
    for l in generate_work_isbns(xml_pathfile):
        n += 1
        if max_nb_line != -1 and n >= max_nb_line:
            break
        f.write("%s|%s|%s\n" % (l[0], l[1], audit_id))
    f.close()
    return n









