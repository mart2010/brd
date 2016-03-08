# -*- coding: utf-8 -*-
import logging

import scrapy
from brd.scrapy.items import WorkRefItem
import brd
__author__ = 'mouellet'

logger = logging.getLogger(__name__)

# --ex. of book with multiple authors:
# -- https://www.librarything.com/work/26628
# -- https://www.librarything.com/work/989447
# -- https://www.librarything.com/work/5072307


class WorkReference(scrapy.Spider):
    """
    Spider fetching reference info related to all work-id found in
    self.works_to_harvest
    """
    name = 'workreference'
    allowed_domains = ['www.librarything.com']
    ###########################
    # Control setting
    ###########################
    url_workdetail = 'https://www.librarything.com/work/%s/workdetails'

    def __init__(self, **kwargs):
        """
        param wids : list of work-ids to harvest
        """
        super(WorkReference, self).__init__(**kwargs)
        self.wids = kwargs['works_to_harvest']
        self.dump_filepath = kwargs['dump_filepath']

    def start_requests(self):
        for i in xrange(len(self.wids)):
            if i % 100 == 0:
                logger.info("Requested the %d-th work (out of %d)" % (i, len(self.works_to_harvest)))
            wid = str(self.wids[i]['refid'])
            yield scrapy.Request(self.url_workdetail % wid, callback=self.parse_work, meta={'wid': wid})

    def parse_work(self, response):
        wid = response.url[response.url.index('/work/') + 6: response.url.index('/workdetails')]
        item = WorkRefItem(work_refid=wid)
        logger.debug("Harvesting work detail of %s " % wid)
        # when work has duplicate, link it to "master" (ex. 13001031 is dup of 17829)
        if wid != response.meta['wid']:
            item['dup_refid'] = response.meta['wid']
        table_t = '//table[@id="book_bookInformationTable"]'
        item['title'] = response.xpath(table_t + '/tr[1]/td[2]/b/text()').extract_first()
        item['original_lang'] = response.xpath(table_t + '/tr/td/a[starts-with(@href,"/language.php?")]/text()').extract_first()
        item['ori_lang_code'] = brd.get_marc_code(item['original_lang'])
        other_langs = response.xpath(table_t + '//td[starts-with(text(),"Other language")]/following-sibling::*//text()').extract()
        item['other_lang_title'] = "__&__".join(other_langs)

        # return Mary Ann Shaffer (i.e. fname lname)
        authors = response.xpath('//div[@class="headsummary"]/h2/a/text()').extract()
        author_names = ";".join(authors)
        # Postgres copy_from chokes on occasional 'tab'
        item['authors'] = author_names.replace('\t', '')

        # remove prefix of '/author/shaffermaryan'
        authors_href = response.xpath('//div[@class="headsummary"]/h2/a/@href').extract()
        a_ids = [authors_href[i][8:] for i in range(len(authors_href))]
        item['authors_code'] = ";".join(a_ids)
        all_mds = response.xpath(table_t + '//div[@id="ddcdisplay"]/p[1]/a/text()').extract()
        if len(all_mds) > 0:
            item['mds_code'] = all_mds[0]
            item['mds_text'] = "-->".join(all_mds[1:])

        sub_lines = response.xpath(table_t + '//div[@class="subjectLine"]')
        subjects = []
        for line in sub_lines:
            subs = line.xpath('./a[starts-with(@href,"/subject/")]/text()').extract()
            subjects.append("->".join(subs))
        if len(subjects) > 0:
            item['lc_subjects'] = "__&__".join(subjects)

        pop = response.xpath('//tr[@class="wslcontent"]/td/a[contains(@href,"popularity")]/text()').extract_first()
        # sometimes no link is found for popularity
        if pop is None:
            pop = response.xpath('//tr[@class="wslcontent"]/td[3]/text()').extract_first()
        pop = pop.replace(",", "")
        # sometimes = '--' (probably means not calculated?)
        try:
            item['popularity'] = int(pop)
        except:
            pass

        yield item

    def get_dump_filepath(self):
        return self.dump_filepath

