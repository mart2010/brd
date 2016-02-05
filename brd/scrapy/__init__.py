# -*- coding: utf-8 -*-
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import brd.elt
import datetime

__author__ = 'mouellet'


class SpiderProcessor(object):
    """
    Responsible in triggering Spider session, and has same dependecies as Spider

    """
    def __init__(self, spidername, **params):
        self.spidername = spidername
        self.dump_filepath = params['dump_filepath']
        self.begin_period = params.get('begin_period', None)
        self.end_period = params.get('end_period', None)
        self.works_to_harvest = params['works_to_harvest']
        self.reviews_order = params['reviews_order']
        self.audit_id = params['audit_id']

    def start_process(self):
        """
        Start spider crawls process and block until completion
        :return: dump_filepath
        """
        # use settings.py defined under scrapy package
        process = CrawlerProcess(get_project_settings())

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
        return self.dump_filepath
