# -*- coding: utf-8 -*-
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import brd.elt
import datetime

__author__ = 'mouellet'


class SpiderProcessor(object):
    """
    Class to ease the process of launching harvest spider session
    and manage auditing
    """
    def __init__(self, spidername, **params):
        self.spidername = spidername
        self.dump_filepath = params['dump_filepath']
        self.begin_period = params.get('begin_period', None)
        self.end_period = params.get('end_period', None)
        self.works_to_harvest = params['works_to_harvest']
        self.reviews_order = params['reviews_order']
        self.audit_id = -1

    def start_process(self):
        """
        Start spider crawls process and block until completion
        :return: (audit-id, dump_filepath) of associated harvesting job
        """
        # use settings.py defined under scrapy package
        process = CrawlerProcess(get_project_settings())

        self.audit_id = self._get_audit()
        self.dump_filepath = self.dump_filepath.replace('(audit-id)', '(%d)' % self.audit_id)
        # update step name with audit-id (circular dependency between filename and auditing)
        self._update_step_filename(self.dump_filepath, self.audit_id)

        # instantiate the spider
        process.crawl(self.spidername,
                      audit_id=self.audit_id,
                      dump_filepath=self.dump_filepath,
                      begin_period=self.begin_period,
                      end_period=self.end_period,
                      reviews_order=self.reviews_order,
                      works_to_harvest=self.works_to_harvest)

        # blocks here until crawling is finished
        process.start()

        # TODO: see how getting back spider instance (to capture nb of rows generated)
        self._commit_audit(self.audit_id)
        return (self.audit_id, self.dump_filepath)

    def _get_audit(self):
        audit_id = brd.elt.insert_auditing(job="Harvest " + self.spidername,
                                           step="...",
                                           begin=self.begin_period,
                                           end=self.end_period,
                                           start_dts=datetime.datetime.now())
        return audit_id


    @staticmethod
    def _update_step_filename(filename, audit_id):
            stepname = filename
            brd.elt.get_connection().execute("update staging.load_audit set step_name=%s where id=%s",
                                             (stepname, audit_id))
            return stepname

    @staticmethod
    def _commit_audit(audit_id):
        # no longer know the # of records/line (pipeline knows...)
        brd.elt.update_auditing(commit=True,
                                rows=-1,
                                status="Completed",
                                finish_dts=datetime.datetime.now(),
                                id=audit_id)
