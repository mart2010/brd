# -*- coding: utf-8 -*-
import scrapy

__author__ = 'mouellet'


class WorkReference(scrapy.Spider):

    name = 'workreference'
    # allowed_domains = ['www.librarything.com']

    ###########################
    # Control setting
    ###########################
    url_workdetail = 'https://www.librarything.com/work/%d/workdetails'

    nb_work_to_scrape = 1000
    ##########################

    ###########################
    # Parse setting
    ###########################
    xpath_author_txt = '//div[@class="headsummary"]/h2/a/text()'
    xpath_author_href = '//div[@class="headsummary"]/h2/a/@href'
    xpath_title = '//table[@id="book_bookInformationTable"]/tbody/tr[1]/td[2]/b/text()'
    xpath_ori_lang = '//table[@id="book_bookInformationTable"]/tbody/tr/td/a[starts-with(@href,"/language.php?")]/text()'

    xpath_isbn = '//meta[@property="books:isbn"]/@content'

    ###########################


    def get_next_workid(self, n):
        pass


    def start_requests(self):
        yield scrapy.Request(self.url_nb_reviews % (0, 2), callback=self.parse_work)


    def parse_work(self, response):
        pass




