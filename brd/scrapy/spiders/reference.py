# -*- coding: utf-8 -*-
import scrapy
from brd.scrapy.items import WorkItem

__author__ = 'mouellet'


class WorkReference(scrapy.Spider):
    name = 'workreference'
    allowed_domains = ['www.librarything.com']
    ###########################
    # Control setting
    ###########################
    url_workdetail = 'https://www.librarything.com/work/%d/workdetails'
    nb_work_to_scrape = 1000
    ###########################
    # Parse setting
    ###########################
    xpath_author_txt = '//div[@class="headsummary"]/h2/a/text()'
    xpath_author_href = '//div[@class="headsummary"]/h2/a/@href'
    xpath_title = '//table[@id="book_bookInformationTable"]/tbody/tr[1]/td[2]/b/text()'
    xpath_ori_lang = '//table[@id="book_bookInformationTable"]/tbody/tr/td/a[starts-with(@href,"/language.php?")]/text()'

    xpath_isbn = '//meta[@property="books:isbn"]/@content'
    ###########################

    def __init__(self, ref_workids, **kwargs):
        super(WorkReference, self).__init__(**kwargs)
        self.workids = ref_workids

    def get_next_workid(self, n):
        pass

    def start_requests(self):
        for wid in self.workids:
            yield scrapy.Request(self.url_workdetail % wid, callback=self.parse_work)

    def parse_work(self, response):

        wid = response.url[response.url.index('/work/') + 6: response.url.index('/workdetails')]
        title = response.xpath(self.xpath_title)
        ori_lang = response.xpath(self.xpath_ori_lang)

        sel_authors = response.xpath(self.xpath_author_txt)
        sel_authors_href = response.xpath(self.xpath_author_href)
        if len(sel_authors) != len(sel_authors_href):
            raise Exception('Invalid authors name and href for wid %s ' % wid)

        authors_name = []
        authors_id = []
        for i in range(len(sel_authors)):
            authors_name.append(sel_authors[i].extract())  # return Mary Ann Shaffer (i.e. fname lname)
            authors_id.append(sel_authors_href[i].extract()[8:])  # remove prefix of '/author/shaffermaryan'

        #TODO : the ddc_mds and lc_subjs
        item = WorkItem(work_refid=wid, title=title, original_lang=ori_lang, authors_name=authors_name,
                        authors_disamb_id=authors_id, lc_subjects=6, ddc_mds=7, load_audit_id=8)

        yield item



