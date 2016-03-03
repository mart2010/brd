# -*- coding: utf-8 -*-
import logging

import scrapy
from brd.scrapy.items import WorkRefItem
import brd.scrapy.spiders.reviews as reviews
import brd
__author__ = 'mouellet'

logger = logging.getLogger(__name__)

# --ex. of book with multiple authors:
# -- https://www.librarything.com/work/26628
# -- https://www.librarything.com/work/989447
# -- https://www.librarything.com/work/5072307


class WorkReference(reviews.BaseSpider):
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

    def start_requests(self):
        i = 0
        for d in self.works_to_harvest:
            i += 1
            if i % 100 == 0:
                logger.info("Requested the %d-th work (out of %d)" % (i, len(self.works_to_harvest)))

            wid = str(d['refid'])
            yield scrapy.Request(self.url_workdetail % wid, callback=self.parse_work, meta={'wid': wid})


    def parse_work(self, response):
        wid = response.url[response.url.index('/work/') + 6: response.url.index('/workdetails')]
        logger.info("about to harvest work %s " % wid)
        dup_id = None
        # when work has duplicate, link it to "master" (ex. 13001031 is dup of 17829)
        if wid != response.meta['wid']:
            dup_id = response.meta['wid']

        table_t = '//table[@id="book_bookInformationTable"]'
        title = response.xpath(table_t + '/tr[1]/td[2]/b/text()').extract_first()
        ori_lang = response.xpath(table_t + '/tr/td/a[starts-with(@href,"/language.php?")]/text()').extract_first()
        ori_lang_code = brd.get_marc_code(ori_lang)
        sel_authors = response.xpath('//div[@class="headsummary"]/h2/a/text()')
        sel_authors_href = response.xpath('//div[@class="headsummary"]/h2/a/@href')
        if len(sel_authors) != len(sel_authors_href):
            raise Exception('Invalid authors name and href for wid %s ' % wid)
        # return Mary Ann Shaffer (i.e. fname lname)
        a_names = [sel_authors[i].extract() for i in range(len(sel_authors))]
        author_names = ";".join(a_names)
        # remove prefix of '/author/shaffermaryan'
        a_ids = [sel_authors_href[i].extract()[8:] for i in range(len(sel_authors))]
        author_ids = ";".join(a_ids)
        all_mds = response.xpath(table_t + '//div[@id="ddcdisplay"]/p[1]/a/text()').extract()
        if len(all_mds) > 0:
            mds_code = all_mds[0]
            mds_text = "-->".join(all_mds[1:])
        else:
            mds_code = None
            mds_text = None

        sub_lines = response.xpath(table_t + '//div[@class="subjectLine"]')
        subjects = []
        for line in sub_lines:
            subs = line.xpath('./a[starts-with(@href,"/subject/")]/text()').extract()
            subjects.append("->".join(subs))
        if len(subjects) > 0:
            lc_subjects = " :: ".join(subjects)
        else:
            lc_subjects = None
        pop = response.xpath('//tr[@class="wslcontent"]/td/a[contains(@href,"popularity")]/text()').extract_first()
        # sometimes no link is found for popularity
        if pop is None:
            pop = response.xpath('//tr[@class="wslcontent"]/td[3]/text()').extract_first()
        popularity = int(pop.replace(",", ""))
        yield WorkRefItem(work_refid=wid, dup_refid=dup_id, title=title, original_lang=ori_lang, ori_lang_code=ori_lang_code,
                          authors=author_names, authors_code=author_ids, lc_subjects=lc_subjects, mds_code=mds_code,
                          mds_text=mds_text, popularity=popularity)

