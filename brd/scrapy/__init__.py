# -*- coding: utf-8 -*-
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import brd.elt
import datetime

__author__ = 'mouellet'


class SpiderProcessor(object):
    """
    Responsible in triggering Spider session and feeding dependencies as Spider

    """
    def __init__(self, spidername, **params):
        self.spidername = spidername
        self.dump_filepath = params['dump_filepath']
        self.works_to_harvest = params['works_to_harvest']

    def start_process(self):
        """
        Start spider crawls process and block until completion
        :return: dump_filepath
        """
        # use settings.py defined under scrapy package
        process = CrawlerProcess(get_project_settings())

        # instantiate the spider
        r = process.crawl(self.spidername, dump_filepath=self.dump_filepath, works_to_harvest=self.works_to_harvest)
        print "Le tyyyyyyyyyyyyyppppppppppeeeeeeeee retourne du crawl est : " + str(type(r)) + " et son content" + str(r)

        # blocks here until crawling is finished
        process.start()
        return self.dump_filepath
