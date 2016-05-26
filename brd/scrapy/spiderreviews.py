# -*- coding: utf-8 -*-
__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The BRD Project"


import logging

import brd
from brd.scrapy.scrapy_utils import resolve_value
import json
import scrapy
import scrapy.exceptions
from brd.scrapy.items import ReviewItem
import brd.scrapy.scrapy_utils as scrapy_utils
import datetime
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

__author__ = 'mouellet'

logger = logging.getLogger(__name__)

class BaseReviewSpider(scrapy.Spider):
    """
    Superclass of all review spider

    """
    # to convert review Date string (to define in subclass)
    raw_date_format = None
    min_harvest_date = datetime.datetime(1900, 1, 1).date()

    def __init__(self, **kwargs):
        """
        Arguments passed to all Spider subclass harvesting review

        Spiders do NOT manage any load logic, they're only concerned
        with harvesting review (delegating outputing to pipelines)
        1. dump_filepath
        2. to_date: harvest only review created before to_date (exclusive)
        3. works_to_harvest: list of dict with  work-ids (and additional info):
        [{'work_refid': x,
        'work_uid': yy (the other site work id to map lt's work_refid with)
        'last_harvest_date': y,
        'nb_in_db': {'ENG': 12, 'FRE': 2, ..},
        'isbns': [x,y,..]}, {..}...]
        n.b. certain keywords are spider/context specific (ex. isbns...)
        """
        super(BaseReviewSpider, self).__init__(**kwargs)
        self.dump_filepath = kwargs['dump_filepath']
        self.works_to_harvest = kwargs['works_to_harvest']

        if type(kwargs['to_date']) == datetime.date:
            self.to_date = kwargs['to_date']
        elif type(kwargs['to_date']) == datetime.datetime:
            self.to_date = kwargs['to_date'].date()
        else:
            raise ValueError('Unexpected to_date parameter %s' % kwargs['to_date'])

    def build_review_item(self, **kwargs):
        item = ReviewItem(site_logical_name=self.name, **kwargs)
        return item

    def get_dump_filepath(self):
        return self.dump_filepath

    def parse_review_date(self, raw_date):
        return datetime.datetime.strptime(raw_date, self.raw_date_format).date()

    def parse_review_date(self, raw_date):
        return datetime.datetime.strptime(raw_date, self.raw_date_format).date()

    def detect_review_lang(self, review):
        lang_code = u'und'
        if not review:
            return None
        elif len(review) < 4:
            return lang_code
        else:
            try:
                lang_code = detect(review)
            except LangDetectException:
                pass
            return brd.get_marc_code(lang_code, capital=False)

    def within_harvestperiod(self, item, last_harvest_date):
        """
        Is review_date within period to harvest, return:
         = -1 when review_date is older than last_harvest_date (exlusive)
         =  0 when review_date within last_harvest_date (inclusive) and self.to_date (exclusive)
         =  1 when review_date newer than self.to_date (inclusive)
        """
        if type(last_harvest_date) == datetime.date:
            from_date = last_harvest_date
        elif type(last_harvest_date) == datetime.datetime:
            from_date = last_harvest_date.date()
        else:
            raise ValueError('last_harvest_date must be date or datetime (%s)' % last_harvest_date)
        if item['parsed_review_date'] < from_date:
            return -1
        elif from_date <= item['parsed_review_date'] < self.to_date:
            return 0
        else:
            return 1

    def parse_rating(self, rating):
        raise NotImplementedError


class LibraryThing(BaseReviewSpider):
    """
    Spider reviews in desc order (latest-first) so only needed ones are taken.
    TODO:  revisit this and simplify incremental relying only on last_harvest_date and iteratively
    request a small nb of newest reviews (ex. 20 per lang found) until reviews
    are older than last_harvest_date

    Used to fetch per language, but of DATA-ISSUES the proper language is not 100% right (ex. work-id=11883),
    so now fetch 'All languages' and detect language manually.

    """
    name = 'librarything'
    allowed_domains = ['www.librarything.com']
    # Nov 22, 2012
    raw_date_format = '%b %d, %Y'
    ###########################
    # Control setting
    ###########################
    url_mainpage = 'https://www.librarything.com/work/%s'
    url_formRequest = 'https://www.librarything.com/ajax_profilereviews.php'
    form_static = {'offset': '0', 'type': '3', 'container': 'wp_reviews', 'sort': '0'}
    # 'offset': 25 (i.e. skip first 25.. showing 26 to 50), 'sort': '0'  (0=desc, 3=asc)
    # to set dynamically: 'showCount':25, 'languagePick':'fre' ('all' for all languages), 'workid': '2371329'
    # 'showCount': 25 (i.e. show 25 reviews, show all is set to 10'000 in lt)
    # other formData not mandatory:  bookid: , optionalTitle:, uniqueID: , mode: profile
    ##########################

    def start_requests(self):
        for i in range(len(self.works_to_harvest)):
            wid = self.works_to_harvest[i]['work_refid']
            yield scrapy.Request(self.url_mainpage % wid, callback=self.parse_mainpage, meta={'work_index': i})

    def parse_mainpage(self, response):
        work_index = response.meta['work_index']
        requested_wid = self.works_to_harvest[work_index]['work_refid']
        wid = response.url[response.url.index('/work/') + 6:]
        dup_id = None
        # check for duplicate (in case it was not picked up during ref harvest)
        if int(wid) != int(requested_wid):
            dup_id = requested_wid

        tags_t = []
        tags_n = []
        for tag_sel in response.xpath('//div[@class="tags"]/span'):
            one_t = tag_sel.xpath('.//a/text()').extract_first()
            if one_t:
                tags_t.append(one_t)
                # returns ' (1)'
                np = tag_sel.xpath('./span[@class="count"]/text()').extract_first()
                tags_n.append(scrapy_utils.digit_in_parenthesis(np))

        item = self.build_review_item(work_refid=wid, dup_refid=dup_id)
        if len(tags_t) > 0:
            item['tags_t'] = u"__&__".join(tags_t)
            item['tags_n'] = u";".join(tags_n)
            # tag in eng (lt translates them, except for intl sites like lt.de...)
            item['tags_lang'] = u'eng'

        nrev_text = response.xpath('//tr[@class="wslcontent"]/td/a[contains(@href,"/reviews")]/text()').extract_first()
        if nrev_text:
            last_harvest_date = self.works_to_harvest[work_index].get('last_harvest_date')
            # initial harvest
            if last_harvest_date is None:
                self.works_to_harvest[work_index]['last_harvest_date'] = self.min_harvest_date
                # top-level #OfReviews is sometimes smaller
                to_fetch = int(nrev_text) + 20
                yield scrapy.FormRequest(self.url_formRequest,
                                         formdata=dict(self.form_static, workid=wid, languagePick='all', showCount=str(to_fetch)),
                                         meta={'work_index': work_index, 'passed_item': item},
                                         callback=self.parse_reviews)
            else:
                raise NotImplementedError('incremental harvest not implemented')
        else:
            logger.info("No reviews found for work-refid= %s" % wid)
            yield item


    # For incremental: adapt this callback so it calls back itself by triggering FormRequest
    # with offset/page as long as there are reviews newer than last_harvest_date
    def parse_reviews(self, response):
        widx = response.meta['work_index']
        last_harvest_date = self.works_to_harvest[widx]['last_harvest_date']
        passed_item = response.meta['passed_item']

        for review_sel in response.xpath('//div[@class="bookReview"]'):
            item = self.extract_onereview(passed_item, review_sel)
            if self.within_harvestperiod(item, last_harvest_date) == 0:
                yield item

    def extract_onereview(self, passed_item, rev_sel):
        new_item = dict(passed_item)
        sel1 = rev_sel.xpath('./div[@class="commentText"]')
        all_text = sel1.xpath('.//text()').extract()
        review_t = u' '.join(all_text).strip()
        new_item['review'] = review_t.replace(u' (   )', u'')

        # http://pics..../ss6.gif
        r = sel1.xpath('./span[@class="rating"]/img/@src').extract_first()
        if r:
            new_item['rating'] = r[r.rindex(u'pics/') + 5:]  # gives ss10.gif
            new_item['parsed_rating'] = self.parse_rating(new_item['rating'])

        date_user_sel = rev_sel.xpath('./div[@class="commentFooter"]/span[@class="controlItems"]')
        # /profile/yermat
        username = date_user_sel.xpath('./a[starts-with(@href,"/profile/")]/@href').extract_first()
        new_item['username'] = username[username.rindex(u'/') + 1:]
        # for lt, username and userid are the same
        new_item['user_uid'] = new_item['username']
        # gives :   |  Nov 22, 2012  |
        rdate = date_user_sel.xpath('./text()').extract_first()
        new_item['review_date'] = rdate[rdate.index(u'|') + 1:rdate.rindex(u'|')].strip()
        new_item['parsed_review_date'] = self.parse_review_date(new_item['review_date'])
        new_item['likes'] = rev_sel.xpath('.//span[@class="reviewVoteCount"]/text()').extract_first()
        if new_item['likes']:  # and len(item['likes'] >= 1) and item['likes'].find(u'nbsp') == -1:
            try:
                new_item['parsed_likes'] = int(new_item['likes'])
            except ValueError:
                pass
        # now always detect language manually
        new_item['review_lang'] = self.detect_review_lang(new_item['review'])
        return new_item

    def parse_rating(self, rating):
        parsed_rating = None
        if rating:
            parsed_rating = int(rating[rating.index(u'ss') + 2:rating.index(u'.gif')])
        return parsed_rating


class Amazon(BaseReviewSpider):
    """
    Spider searches all isbns at once for initial load (TODO: incremental load)

    The main edition usually has most of the reviews, while less known edition may have
    just a few (ex. foreign-lang ed), but at this layer we keep everything.

    We keep edition/reviews having diff nb of reviews/avg rating to avoid harvesting same reviews
    for popular work that were merged (for reviews).  Only drawback is to miss distinct review
    done on a diff edition but with same rating (fine these must have very small # of reviews)

    TODO: make generic superclass and create subclass for different top-level domain
    TODO: resolve issues with Robot check on search result..

    """
    name = 'amazon.com'
    # make international sites as subclass (.com.au, .ca, .fr, .in, .co.jp, .nl, .co.uk)
    allowed_domains = ['www.amazon.com']
    # on November 22, 2012
    raw_date_format = 'on %B %d, %Y'

    # isbn must be | separated; sort on customer reviews results in only first lines having reviews..)
    search_url = 'http://www.amazon.com/gp/search/ref=sr_adv_b/?search-alias=stripbooks&unfiltered=1&field-isbn=%s' \
                 '&sort=reviewrank_authority&Adv-Srch-Books-Submit.x=35&Adv-Srch-Books-Submit.y=5'   # + a bunch of empty param
    # %s is placeholder for 'asin'
    review_url = 'http://www.amazon.com/product-reviews/%s/ref=cm_cr_dp_see_all_btm?ie=UTF8&showViewpoints=1&sortBy=recent'

    # check to send formRequest instead with :  sortBy: recent, pageNumber: x, pageSize: 500 (whatever max..), asin: xxyyxx, scope: reviewAjax2
    # other unused param:  reviewerType, formatType, filter*, shouldAppend: undefined, deviceType: desktop, reftag: undefined_2
    #scrapy.FormRequest("http://www.amazon.com/ss/customer-reviews/ajax/reviews/get/ref=undefined_2",
    #                   formdata={'sortBy': 'recent', 'pageNumber': '1', 'pageSize': '100', 'asin': 'xxyyxx', 'scope': 'reviewAjax2'})
    # could be an alternative (to check the pageSize as it seems only max 50 reviews can be returned)

    def start_requests(self):
        for i in range(len(self.works_to_harvest)):
            isbns = self.works_to_harvest[i].get('isbns')
            if isbns:
                self.works_to_harvest[i]['last_harvest_date'] = self.min_harvest_date
                # limit to 100 to avoid too long url (error 400)
                # only trigger first 100th to avoid duplicating harvest from diff request thread
                sub_isbns = isbns[0:100]
                yield scrapy.Request(self.search_url % "|".join(sub_isbns),
                                     meta={'work_index': i},
                                     callback=self.parse_search_resp)
            # incremental loading
            else:
                work_uid = self.works_to_harvest[i]['work_uid']
                last_harvest_date = self.works_to_harvest[i]['last_harvest_date']
                raise NotImplementedError()

    def parse_search_resp(self, response):
        widx = response.meta['work_index']
        work_refid = self.works_to_harvest[widx]['work_refid']
        #TODO: check for page robot check and stop all harvest jobs if so
        if "find" == "a robot":
            raise scrapy.exceptions.CloseSpider("Spider shutsdown due to AZ robot check (at work_index=%d)" % widx)

        no_result = int(response.xpath('boolean(//h1[@id="noResultsTitle"])').extract_first())
        if no_result == 1:
            logger.info("Nothing found for work-refid %s" % (work_refid))
            yield self.build_review_item(work_refid=work_refid, work_uid='-1')
        else:
            # TODO: validate if robot check, and stop processing!
            # dic to hold: {(nb_rev, avg_rev): 'asin'}
            n_dic = {}
            for res in response.xpath('//li[starts-with(@id,"result_")]'):
                asin = res.xpath('./@data-asin').extract_first()
                star_sel = res.xpath('.//div[@class="a-column a-span5 a-span-last"]')
                a_stars = star_sel.xpath('./div[@class="a-row a-spacing-mini"]//span[@class="a-icon-alt"]/text()').extract_first()
                # there are reviews  (ex: 4.3 out of 5 stars)
                if a_stars:
                    avg_stars = a_stars[:a_stars.index(u'out of 5 stars')]
                    star_and_nrevs = star_sel.xpath('./div[@class="a-row a-spacing-mini"]/a[@class="a-size-small a-link-normal a-text-normal"]')
                    nb_reviews = int(star_and_nrevs.xpath('./text()').extract_first().replace(u',', u''))
                    # RULE: same (nb,avg) correspond to duplicates as editions may be merged
                    if (nb_reviews, avg_stars) not in n_dic:
                        n_dic[(nb_reviews, avg_stars)] = asin
            if len(n_dic) > 0:
                for tu in n_dic:
                    yield scrapy.Request(self.review_url % n_dic[tu],
                                         meta={'work_index': widx},
                                         callback=self.parse_reviews)
            else:
                logger.info("No reviews found for work-refid= %s" % work_refid)
                # generate item without tying to any 'asin' to avoid re-searching sames isbns
                yield self.build_review_item(work_refid=work_refid, work_uid='-2')

    def parse_reviews(self, response):
        widx = response.meta['work_index']
        u = response.url
        work_uid = u[u.index('/product-reviews/') + 17:u.index('/ref=')]
        title = response.xpath('//div[@class="a-row product-title"]/span/a/text()').extract_first()
        author = response.xpath('//div[@class="a-row product-by-line"]/a/text()').extract_first()
        item = self.build_review_item(authors=author,
                                      title=title,
                                      work_refid=self.works_to_harvest[widx]['work_refid'],
                                      work_uid=work_uid)
        found_older = False
        all_revs_sel = response.xpath('//div[@class="a-section review"]')
        for rev in all_revs_sel:
            ret_item = self.extract_onereview(item, rev)
            comp = self.within_harvestperiod(ret_item, self.works_to_harvest[widx]['last_harvest_date'])
            if comp == 0:
                yield ret_item
            elif comp == -1:
                found_older = True
                break

        next_sel = response.xpath('//ul[@class="a-pagination"]/li[@class="a-last"]')
        # as long as there are newer reviews, request next page
        if not found_older and len(next_sel) == 1:
            # get next page url
            next_rq = next_sel.xpath('./a/@href').extract_first()
            yield scrapy.Request("http://www." + self.name + next_rq,
                                 meta={'work_index': widx},
                                 callback=self.parse_reviews)

    def extract_onereview(self, passed_item, rev_sel):
        new_item = dict(passed_item)
        # '5.0 out of 5 stars'
        new_item['rating'] = rev_sel.xpath('.//span[@class="a-icon-alt"]/text()').extract_first()
        new_item['parsed_rating'] = self.parse_rating(new_item['rating'])
        reviewer_sel = rev_sel.xpath('.//a[@class="a-size-base a-link-normal author"]')
        new_item['username'] = reviewer_sel.xpath('./text()').extract_first()
        if new_item['username']:
            # /gp/pdp/profile/A17DPO2KZ0ADA/ref=cm_cr_arp_d_pdp?ie=UTF8
            r = reviewer_sel.xpath('./@href').extract_first()
            new_item['user_uid'] = r[r.index(u'/profile/') + 9:r.index(u'/ref=')]
        else:
            # static 'A customer' having neither id nor username
            new_item['username'] = u'A customer'
            new_item['user_uid'] = u'A customer'
        # Get all text() and joining
        full_t = rev_sel.xpath('.//span[@class="a-size-base review-text"]//text()').extract()
        new_item['review'] = u" ".join(full_t).strip()
        # assume language english (true 99.? % of the time!)
        # TODO: could use langdetect..instead
        new_item['review_lang'] = u'eng'

        # 'on November 30, 2015'
        new_item['review_date'] = rev_sel.xpath('.//span[@class="a-size-base a-color-secondary review-date"]/text()').extract_first()
        new_item['parsed_review_date'] = self.parse_review_date(new_item['review_date'])
        # 3 cases:  "14 people found this helpful", "0 of 2 people found this helpful" vs None!
        # AZ adjust code based on browser (chrome only show first case, but not opera (& spider)!
        ls = rev_sel.xpath('.//span[@class="cr-vote-buttons"]/span[@class="a-color-secondary"]/'
                       'span[@class="a-size-small a-color-secondary review-votes"]/text()').extract_first()
        if ls and ls.find(u'people') > 0:
            ls = ls.replace(u',', u'')
            new_item['likes'] = ls
            if ls.find(u' of ') > 0:
                new_item['parsed_likes'] = int(ls[:ls.index(u' of ')])
                new_item['parsed_dislikes'] = int(ls[ls.index(u' of ') + 4:ls.index(u'people')]) - new_item['parsed_likes']
            else:
                new_item['parsed_likes'] = int(ls[:ls.index(u'people')])
        return new_item

    # '5.0 out of 5 stars'
    def parse_rating(self, rating):
        parsed_rating = None
        score = rating[:rating.index(u'out of') - 1]
        if rating:
            parsed_rating = float(score) * 2
        return parsed_rating


class Goodreads(BaseReviewSpider):
    """
    This requires isbns in self.works_to_harvest for those never harvested,
    and relies only on last_harvest_date to filter needed reviews
    TODO  ISSUES:
            1- with max nb of review page is 100!!! whereas a lot more can exist: how to get missing ones?
            2- review text may only be partial due to inconsitent rules in html tag
    """
    name = 'goodreads'
    allowed_domains = ['www.goodreads.com']
    raw_date_format = '%b %d, %Y'
    ###########################
    # Control setting
    ###########################
    url_search = 'https://www.goodreads.com/search?utf8=%E2%9C%93&query='
    # sort=newest or oldest
    url_review = 'https://www.goodreads.com/book/show/%s?page=%d&sort=%s'

    def start_requests(self):
        for i in range(len(self.works_to_harvest)):
            isbns = self.works_to_harvest[i].get('isbns')
            # initial requires search by isbn to map gr work-uid
            if isbns:
                yield scrapy.Request(self.url_search + str(isbns[0]),
                                     meta={'work_index': i, 'nb_try': 1},
                                     callback=self.parse_search_resp)
            # incremental loading
            else:
                work_uid = self.works_to_harvest[i]['work_uid']
                last_harvest_date = self.works_to_harvest[i]['last_harvest_date']
                raise NotImplementedError()

    def parse_search_resp(self, response):
        widx = response.meta['work_index']
        isbns = self.works_to_harvest[widx]['isbns']
        nb_try = response.meta['nb_try']
        # found, map work_uid and request reviews page
        if response.url.find('/book/show/') != -1:
            gr_work_id = response.url[response.url.index('/book/show/') + 11:]
            # add title/authors (only done for initial QA checks)
            title = response.xpath('//h1[@class="bookTitle"]/text()').extract_first().strip()
            a_raw = response.xpath('//a[@class="authorName"]/child::*/text()').extract()
            authors = ",".join(a_raw)
            pass_item = self.build_review_item(work_refid=self.works_to_harvest[widx]['work_refid'],
                                               work_uid=gr_work_id,
                                               authors=authors,
                                               title=title)
            nb_rev = response.xpath('//a[@class="actionLinkLite"]/span[@class="count"]/span[@class="value-title"]/text()').extract_first()
            if int(nb_rev.replace(',', '')) == 0:
                logger.info("No reviews found for work-refid=%s (uid=%s of site %s)"
                            % (pass_item['work_refid'], pass_item['work_uid'], self.name))
                yield pass_item
            # map gr's id and trigger new Request to have reviews ordered correctly
            else:
                self.works_to_harvest[widx]['last_harvest_date'] = self.min_harvest_date
                # For popular review, GR has partial list, so must order by oldest
                yield scrapy.Request(self.url_review % (gr_work_id, 1, 'oldest'),
                                     meta={'work_index': widx, 'item': pass_item},
                                     callback=self.parse_reviews)
        # not found page
        elif 'Looking for a book?' in response.body:
            if nb_try < len(isbns):
                yield scrapy.Request(self.url_search + str(isbns[nb_try]),
                                     meta={'work_index': widx, 'nb_try': nb_try + 1},
                                     callback=self.parse_search_resp)
            else:
                logger.info("Nothing found for wid: %s, isbns: %s" % (self.works_to_harvest[widx]['work_refid'], str(isbns)))
                yield self.build_review_item(work_refid=self.works_to_harvest[widx]['work_refid'], work_uid='-1')
        else:
            logger.error("Unexpected result page after %d try (search isbn=%s)" % (nb_try, isbns[nb_try - 1]))
            # interactively debug page
            from scrapy.shell import inspect_response
            inspect_response(response, self)

    def parse_reviews(self, response):
        widx = response.meta['work_index']
        item = response.meta['item']
        lastpage_no = response.meta.get('lastpage_no')
        if not lastpage_no:
            # get how many pages of reviews, last page is just before the "next page" link
            pageno_before_next = response.xpath('//a[@class="next_page"]/preceding-sibling::*[1]/text()')
            if len(pageno_before_next) > 0:
                lastpage_no = int(pageno_before_next.extract()[0])
            else:
                lastpage_no = 1
            # also harvest tag (i.e. Genres in GR)
            tag_t = []
            tag_n = []
            for tline in response.xpath('//div[starts-with(@class,"bigBoxContent")]/div[starts-with(@class,"elementList ")]'):
                ts = tline.xpath('./div[@class="left"]/a/text()').extract()
                tag_t.append(u" > ".join(ts))
                # u'2 users'
                nu = tline.xpath('./div[@class="right"]/a/text()').extract_first()
                # Data issues: Tag without nb exist (ex. 'Business Amazon' !!)
                if nu:
                    tag_n.append(nu[:nu.index(u'user')])
                else:
                    tag_n.append(u'0')
            if len(tag_t) > 0:
                item['tags_t'] = u"__&__".join(tag_t)
                item['tags_n'] = u";".join(tag_n)
                item['tags_lang'] = u'eng'

        current_page = int(response.url[response.url.index('?page=') + 6:response.url.index('&sort=')])
        found_older = False

        reviews_sel = response.xpath('//div[starts-with(@id,"review_")]')
        for rev in reviews_sel:
            new_item = self.extract_onereview(item, rev)
            comp_flag = self.within_harvestperiod(new_item, self.works_to_harvest[widx]['last_harvest_date'])
            if comp_flag == 0:
                yield new_item
            elif comp_flag == -1:
                found_older = True
                break

        if current_page <= lastpage_no and not found_older:
            # TODO: arrange the ordering for incremental loading
            yield scrapy.Request(self.url_review % (item['work_uid'], current_page + 1, 'oldest'),
                                 meta={'work_index': widx, 'item': item, 'lastpage_no': lastpage_no},
                                 callback=self.parse_reviews)

    def extract_onereview(self, item, rev):
        """
        Process one review in rev selector
        :return: new instance Item
        """
        new_item = dict(item)
        new_item['review_date'] = rev.xpath('.//a[@itemprop="publishDate"]/text()').extract_first()  # u'Feb 14, 2016'
        new_item['parsed_review_date'] = self.parse_review_date(new_item['review_date'])
        # for gr, 0 star means No rating (however some user consider it as 0 rating!)
        nb_star = len(rev.xpath('.//span[@class="staticStar p10"]'))
        new_item['rating'] = str(nb_star) if nb_star > 0 else None
        new_item['parsed_rating'] = self.parse_rating(new_item['rating'])
        new_item['username'] = rev.xpath('.//a[@class="user"]/@title').extract_first()  # u'Jon Liu'
        u_link = rev.xpath('.//a[@class="user"]/@href').extract_first()  # u'/user/show/52104079-jon-liu'
        new_item['user_uid'] = u_link[u_link.index(u'/show/') + 6:]
        # no clear logical rules: finally it seems complete text is at the last span element...
        r_texts = rev.xpath('.//span[starts-with(@id,"reviewTextContainer")]/span[starts-with(@id,"freeText")][last()]//text()').extract()
        text_t = u" ".join(r_texts).strip()
        # issues with \r\n (^M windows line ending) when Bulk-loading postgres
        new_item['review'] = text_t.replace(u'\r\n', u'\n')
        # for gr, we use automatic lang detection
        new_item['review_lang'] = self.detect_review_lang(new_item['review'])

        likes_raw = rev.xpath('.//span[@class="likesCount"]/text()').extract_first()
        if likes_raw:
            new_item['likes'] = likes_raw
            new_item['parsed_likes'] = int(likes_raw[:likes_raw.index(u'like')])
        return new_item

    def parse_rating(self, rating):
        """
        Normalize gr's rating (which are 5-based star) on a 10-based star
        """
        parsed_rating = None
        if rating:
            parsed_rating = 2 * int(rating)
        return parsed_rating


class Babelio(BaseReviewSpider):
    """
    Babelio has no global list to easily crawl from.  Best approach is to
    search reviews based on ISBNs like goodreads.
    """
    name = 'babelio'
    allowed_domains = ['www.babelio.com']
    ###########################
    # Control setting
    ###########################
    form_search = 'http://www.babelio.com/resrecherche.php'
    # main page used to fetch tags
    url_main = "http://www.babelio.com/livres/%s"
    # Book_uid (ex. 'Green-Nos-etoiles-contraires/436732')
    # tri=dt order by date descending
    param_review = "/critiques?pageN=%d&tri=dt"

    def start_requests(self):
        for i in range(len(self.works_to_harvest)):
            isbns = self.works_to_harvest[i].get('isbns')
            # initial harvest
            if isbns:
                self.works_to_harvest[i]['last_harvest_date'] = self.min_harvest_date
                yield scrapy.FormRequest(self.form_search,
                                         formdata={'Recherche': str(isbns[0]), 'item_recherche': 'isbn'},
                                         meta={'work_index': i, 'nb_try': 1},
                                         callback=self.parse_search_resp)
            # incremental harvest
            else:
                work_uid = self.works_to_harvest[i]['work_uid']
                last_harvest_date = self.works_to_harvest[i]['last_harvest_date']
                raise NotImplementedError()

    def parse_search_resp(self, response):
        widx = response.meta['work_index']
        isbns = self.works_to_harvest[widx]['isbns']
        nb_try = response.meta['nb_try']
        titre_xp = '//td[@class="titre_livre"]'
        res_sel = response.xpath(titre_xp + '/a[@class="titre_v2"]')
        uid_txt = res_sel.xpath('./@href').extract_first()  # u'/livres/Levy-Rien-de-grave/9229'
        # found it
        if uid_txt:
            uid = uid_txt[uid_txt.index(u'/livres/') + 8:]
            title = res_sel.xpath('./text()').extract_first().strip()
            author = response.xpath('//td[@class="auteur"]/a/text()').extract_first().strip()
            pass_item = self.build_review_item(work_refid=self.works_to_harvest[widx]['work_refid'],
                                               work_uid=uid,
                                               title=title,
                                               authors=author)
            nb_t = response.xpath(titre_xp + '/a[contains(@href,"#critiques")]/span/text()').extract_first()
            if not nb_t or int(nb_t) == 0:
                logger.info("No reviews found for work-refid=%s in site %s (uid=%s)"
                        % (pass_item['work_refid'], self.name, pass_item['work_uid']))
                yield pass_item
            # DATA-ISSUES: may indicate 1-review, when none exist (ex. Ballard-La-Course-au-Paradis/230360)
            # the impact: try many times to harvest same work as no reviews will be yielded
            else:
                yield scrapy.Request(self.url_main % uid,
                                     meta={'work_index': widx, 'pass_item': pass_item, 'main_page': True},
                                     callback=self.parse_reviews)
        else:
            n_found = response.xpath('//div[@class="content row"]//div[@class="titre"]/text()').extract_first()
            # found no book
            if n_found and n_found.find(u'(0)') != -1:
                if nb_try < len(isbns):
                    yield scrapy.FormRequest(self.form_search,
                                             formdata={'Recherche': str(isbns[nb_try]), 'item_recherche': 'isbn'},
                                             meta={'work_index': widx, 'nb_try': nb_try + 1},
                                             callback=self.parse_search_resp)
                else:
                    logger.info("Nothing found for isbn=%s in site %s" % (isbns[nb_try - 1], self.name))
                    yield self.build_review_item(work_refid=self.works_to_harvest[widx]['work_refid'], work_uid='-1')
            else:
                logger.error("Unexpected result page after %d try (search isbn=%s)" % (nb_try, isbns[nb_try - 1]))
                # interactively debug page
                from scrapy.shell import inspect_response
                inspect_response(response, self)

    def parse_reviews(self, response):
        widx = response.meta['work_index']
        item = response.meta['pass_item']

        if response.meta.get('main_page'):
            # it may happen that uid taken from search result is different (request was redirected)
            if response.meta.get('redirect_urls'):
                item['work_uid'] = response.url[response.url.index('/livres/')+8:]
            # fetch tags from main page
            tags_sel = response.xpath('//p[@class="tags"]/a[@rel="tag"]')
            tags_t = []
            tags_n = []
            # only way to get approx. frequency ia through font-size
            for tag_s in tags_sel:
                tags_t.append(tag_s.xpath('./text()').extract_first().strip())
                # "tag_t17 tc0 ..."
                tag_n_c = tag_s.xpath('./@class').extract_first()
                tags_n.append(tag_n_c[5:tag_n_c.index(u' ')])

            item['tags_t'] = u"__&__".join(tags_t)
            item['tags_n'] = u";".join(tags_n)

            item['tags_lang'] = u'fre'

            # request first review page
            yield scrapy.Request((self.url_main + self.param_review) % (item['work_uid'], 1),
                                 meta={'work_index': widx, 'pass_item': item},
                                 callback=self.parse_reviews)
        # collect from reviews page
        else:
            last_page = response.meta.get('last_page')
            if last_page is None:
                page_row = response.xpath('//div[@class="pagination row"]')
                if len(page_row) == 0:
                    last_page = 1
                else:
                    last_page = int(page_row.xpath('./a[last()-1]/text()').extract_first())
            # used for debugging... could be removed
            if response.url.find('?pageN=') == -1 or response.url.find('&tri=') == -1:
                from scrapy.shell import inspect_response
                inspect_response(response, self)

            cur_page = int(response.url[response.url.index('?pageN=') + 7:response.url.index('&tri=')])
            found_older = False
            reviews_sel = response.xpath('//div[@class="post_con"]')
            for rev in reviews_sel:
                new_item = self.extract_onereview(item, rev)
                comp = self.within_harvestperiod(new_item, self.works_to_harvest[widx]['last_harvest_date'])
                if comp == -1:
                    found_older = True
                    break
                elif comp == 0:
                    yield new_item
            if cur_page < last_page and not found_older:
                yield scrapy.Request((self.url_main + self.param_review) % (item['work_uid'], cur_page + 1),
                                     meta={'work_index': widx, 'last_page': last_page, 'pass_item': item},
                                     callback=self.parse_reviews)

    def extract_onereview(self, passed_item, rev):
        """
        Process one review in rev selector.
        :return: new Item generated
        """
        item = dict(passed_item)
        user_sel = rev.xpath('.//a[contains(@href,"/monprofil.php?id_user=") and @class="author"]')
        u = user_sel.xpath('./@href').extract_first()  # u'/monprofil.php?id_user=221169'
        item['user_uid'] = u[u.index(u'id_user=') + 8:]
        item['username'] = user_sel.xpath('./text()').extract_first()
        d = rev.xpath('.//td[@class="no_img"]/span/text()').extract_first()  # u'22 f\xe9vrier 2016'
        item['review_date'] = d
        item['parsed_review_date'] = self.parse_review_date(d)
        # babelio has 0 to 5 stars with no half-star
        star = rev.xpath('.//li[@class="current-rating"]/text()').extract_first()  # u'Livres 4.00/5'
        # it seems no star really mean rating = 0
        if star:
            item['rating'] = star[star.index(u"Livres ") + 7:star.index(u"/")]
            item['parsed_rating'] = int(float(item['rating'])) * 2
        else:
            item['parsed_rating'] = 0
        item['review_lang'] = u'fre'
        lines = rev.xpath('.//div[@class="text row"]/div/text()').extract()
        item['review'] = u" ".join(lines).strip()
        item['likes'] = rev.xpath('.//span[@class="post_items_like "]/span[@id]/text()').extract_first()
        item['parsed_likes'] = int(item['likes'])

        return item

    def parse_review_date(self, raw_date):
        mois = raw_date[raw_date.index(" ") + 1: raw_date.rindex(" ")]
        month_nb = int(scrapy_utils.mois[mois])
        day = int(raw_date[:raw_date.index(" ")])
        year = int(raw_date[raw_date.rindex(" ") + 1:])
        return datetime.datetime(year, month_nb, day).date()

class CritiquesLibres(BaseReviewSpider):
    """
    First Step: Fetch # of Reviews in Json format using request:
        # http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=0&limit=300
        (adjust: start=? and limit=?)

        Json :  {"total":"44489", "data":[ {"id":"", "titre":"", "nbrcrit":"", ...}, {"id":....}] }
        where total is the total number of books reviewed.

    Second step:  Fetch new reviews ready for loading.
    Note:
    1) takes only 4 min to harvest all reviews (period 2000-2014), so can be relauched
    in a single session without messing around with incremental harvesting
    """
    name = 'critiqueslibres'
    allowed_domains = ['www.critiqueslibres.com']  # used as 'hostname' in item field
    lang = 'FR'

    ###########################
    # Control setting
    ###########################
    items_per_page = 2000
    # set to -1, for proper initialization (for test purposes, set to any small values)
    max_nb_items = 10000  # -1
    ##########################

    ###########################
    # Parse_nb_reviews setting
    ###########################
    url_nb_reviews = "http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=%d&limit=%d"
    xpath_allreviews = '//table[@width="100%" and @border="0" and @cellpadding="3"]/tr'  # for Review page (is there more stable xpath?)
    ##########################

    ###########################
    # Parse_review setting
    ###########################
    url_review = "http://www.critiqueslibres.com/i.php/vcrit/%d?alt=print"
    xpath_title = '//td[@class="texte"]/p/strong/text()'
    xpath_rating = './td[@class="texte"]/img[contains(@name,"etoiles")]/@name'
    xpath_date = './td[@class="texte"]/p[2]/text()'  # return:  " - BOURGES - 50 ans - 1 décembre 2004"
    xpath_username = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/text()'
    xpath_reviewer_uid = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/@href'  # return: "/i.php/vuser/?uid=32wdqee2334"
    ###########################

    def init_max_nb_items(self, response):
        res = json.loads(response.body[1:-1])
        self.max_nb_items = int(res['total'])

    def start_requests(self):
        if self.max_nb_items == -1:
            yield scrapy.Request(self.url_nb_reviews % (0, 2), callback=self.init_max_nb_items)

        for i in xrange(0, self.max_nb_items, self.items_per_page):
            yield scrapy.Request(self.url_nb_reviews % (i, self.items_per_page), callback=self.parse_nb_reviews)

    def parse_nb_reviews(self, response):
        pageres = json.loads(response.body[1:-1])

        if pageres['success'] != 0:
            raise IOError("Page result error for request: " + response.request)

        for review in pageres['data']:
            # Only reviews-eclaire considered for scraping (i.e. = #reviews - 1 )
            nreviews_eclair = int(review['nbrcrit']) - 1
            bookuid = review['id']
            # only crawls work with reviews not yet persisted
            if nreviews_eclair > 1:  # TODO: implement same logic as other with works_to_harvest instead:
                lname, fname = self.parse_author(review['auteurstring'])
                item = self.build_review_item(book_uid=bookuid,
                                  title=review['titre'],
                                  book_lang=self.lang,
                                  author_fname=fname,
                                  author_lname=lname)
                # trigger the 2nd Request
                request = scrapy.Request(self.url_review % int(bookuid), callback=self.parse_review)
                request.meta['item'] = item
                yield request

    def parse_review(self, response):
        passed_item = response.meta['item']

        if resolve_value(response.selector, self.xpath_title) != passed_item['title']:
            raise ValueError("Book title in webpage ('%s') different from Json ('%s')"
                             % (
                             resolve_value(response.selector, self.xpath_title), response.meta['item']['title']))

        allreviews = response.xpath(self.xpath_allreviews)
        rowno = 1
        for review_sel in allreviews:
            # iterate through 3 rows for each critics : row1: title_star, row2: username + date, row3: horizontal line
            if rowno == 1:
                passed_item['rating'] = resolve_value(review_sel, self.xpath_rating)
                # TODO: add the review which is now the review content as text
                rowno = 2
            elif rowno == 2:
                ruid = resolve_value(review_sel, self.xpath_reviewer_uid)
                rdate = resolve_value(review_sel, self.xpath_date)
                passed_item['username'] = resolve_value(review_sel, self.xpath_username),
                passed_item['user_uid'] = ruid[ruid.rfind(u"=") + 1:],
                passed_item['review_date'] = rdate[rdate.rfind(u"-") + 2:]
                rowno = 3
            else:
                rowno = 1
                yield passed_item

    def parse_review_date(self, review_date_str):
        month_name = review_date_str[(review_date_str.find(u' ') + 1): review_date_str.rfind(u' ')]
        month_no = scrapy_utils.mois[month_name]
        review_date_str = review_date_str.replace(month_name, month_no)
        return datetime.strptime(review_date_str, '%d %m %Y').date()

    def parse_author(self, author_str):
        i = author_str.index(', ')
        lname = author_str[0:i]
        fname = author_str[i + 2:]
        return (lname, fname)


class DecitreSpider(BaseReviewSpider):
    """
    This is similar to Babelio, only a search can be easily implemented
    """
    pass


