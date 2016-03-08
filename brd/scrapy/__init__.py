# -*- coding: utf-8 -*-
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


__author__ = 'mouellet'


class SpiderProcessor(object):
    """
    Responsible in triggering Spider session and feeding dependencies as Spider

    """
    def __init__(self, spidername, **kwargs):
        self.spidername = spidername
        self.kwargs = kwargs

    def start_process(self):
        """
        Start spider crawls process and block until completion
        :return: dump_filepath
        """
        # use settings.py defined under scrapy package
        process = CrawlerProcess(get_project_settings())

        # instantiate the spider
        process.crawl(self.spidername, **self.kwargs)
        # blocks here until crawling is finished
        process.start()
        # clean termination
        # process.stop()
        return self.kwargs['dump_filepath']
