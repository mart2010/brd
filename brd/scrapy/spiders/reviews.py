# -*- coding: utf-8 -*-
import brd

__author__ = 'mouellet'

from brd.scrapy.scrapy_utils import resolve_value
import json
import scrapy
from brd.scrapy.items import ReviewItem
import brd.scrapy.scrapy_utils as scrapy_utils
from datetime import datetime


class BaseAuditSpider(scrapy.Spider):
    """
    All sub-class spiders must have dump_filepath to know where to save output file
     as well as audit_id for linking harvested items with log audit in DB
    """
    def __int__(self, **kwargs):
        """
        Parameters :
        1. audit_id: id of associated harvest Batch record in load_audit
        2. dump_filepath:
        """
        super(BaseAuditSpider, self).__init__(**kwargs)
        self.dump_filepath = kwargs['dump_filepath']
        self.audit_id = kwargs['audit_id']

    def get_audit_id(self):
        return self.audit_id

    def get_dump_filepath(self):
        return self.dump_filepath


class BaseReviewSpider(BaseAuditSpider):
    # To be tuned by subclass (ltreview may use diff value as it impacts Work-set of other spiders)
    # min_required_review = 5

    def __init__(self, **kwargs):
        """
        Arguments passed to all Spider subclass harvesting review

        Spiders do NOT manage any load logic (initial-harvest, incremental-harvest etc..), only concerned
        with harvesting review (delegating filtering/saving to pipelines)

        1. param begin_period: Review's min Date to harvest

        2. param end_date: Review's max Date to harvest (exclusive)

        3. param works_to_harvest: contain work-ids to request along with additional info
        (not all work are harvested in one session):
                [{'work-ori-id': idXXX, 'last_harvest_date': dateX, 'nb_in_db': {'ENG': 12, 'FRE': 2, ..}}, ...]

        4. param reviews_order: spider can request reviews page in 'asc' or 'desc' of review_date (optional)


        """
        super(BaseReviewSpider, self).__init__(**kwargs)
        self.begin_period = kwargs['begin_period']
        self.end_period = kwargs['end_period']
        # descending order is the default
        self.reviews_order = kwargs.get('reviews_order', 'desc')
        self.works_to_harvest = kwargs.get('works_to_harvest', {})

    def build_review_item(self):
        return ReviewItem(load_audit_id=self.get_audit_id(),
                          site_logical_name=self.name)

    def parse_review_date(self, review_date_str):
        raise NotImplementedError


class LibraryThingWorkReview(BaseReviewSpider):
    """
    For each Work, spider only fetches #of reviews needed (by comparing #of review on Page vs DB).
    Reviews are then requested in chronological order (latest-first) so that only the first needed ones
    are emitted.  Hence this spider doesn't need these param:
        1) Begin_period (unless we need to do data integrity checks)
        2) Reviews_order
    End_period is used so not to harvest too recent review (needed if lt allows for update -to validate?)
    """
    name = 'librarything'
    allowed_domains = ['www.librarything.com']
    # flag indicating site have multi-language reviews
    lang = 'ALL'

    ###########################
    # Control setting
    ###########################
    url_workreview = 'https://www.librarything.com/work/%s/reviews'
    url_formRequest = 'https://www.librarything.com/ajax_profilereviews.php'

    form_static_data = {'offset': '0', 'type': '3', 'container': 'wp_reviews'}
    # 'offset': 25 (i.e. skip first 25.. showing 26 to 50)
    # to set dynamically: 'showCount':25, 'languagePick':'fre', 'workid': '2371329', 'sort': '0'  (0=desc, 3=asc)
    # 'showCount': 25 (i.e. show 25 reviews, show all is set to 10'000 in lt)
    # other formData not mandatory:  bookid: , optionalTitle:, uniqueID: , mode: profile
    ##########################

    ###########################
    # Parse setting
    ###########################
    langs_root = '//div[@class="languagepick"]//text()'
    xpath_reviews = '//div[@class="bookReview"]'
    xpath_rtext_rating = './div[@class="commentText"]'
    xpath_user_date = './div[@class="commentFooter"]/span[@class="controlItems"]'
    ###########################

    def start_requests(self):
        for i in xrange(len(self.works_to_harvest)):
            wid = self.works_to_harvest[i]['work-ori-id']
            req = scrapy.Request(self.url_workreview % wid, callback=self.parse_nbreview)
            req.meta['work-index'] = i
            yield req

    def parse_nbreview(self, response):

        nb_buffer = 5

        def prepare_form(workid, langpick, n_in_page, n_in_db):
            """
            Using descending sort and showCount = nb-missing from DB
            guarantees incremental/initial harvesting to work properly
            """
            form_data = dict(self.form_static_data)
            form_data['workid'] = workid
            form_data['languagePick'] = langpick
            form_data['sort'] = '0'  # descending
            nb_missing = n_in_page - n_in_db + nb_buffer
            form_data['showCount'] = str(nb_missing)
            return form_data

        wid = response.url[response.url.index('/work/') + 6: response.url.index('/reviews')]
        work_index = response.meta['work-index']
        db_info = self.works_to_harvest[work_index]
        nb_db_dic = db_info.get('nb_in_db', {})
        nb_page_site = self.scrape_langs_nb(response)
        # now only need this for data integrity checks
        last_harvest_date_db = db_info.get('last_harvest_date', None)

        for lang in nb_page_site:
            marc_code = brd.get_marc_code(lang, capital=False)
            nb_in_db = nb_db_dic.get(marc_code, 0)
            nb_in_site = nb_page_site[lang]
            if last_harvest_date_db is None or nb_in_site > nb_in_db:
                r = scrapy.FormRequest(self.url_formRequest,
                                       formdata=prepare_form(wid, marc_code, nb_in_site, nb_in_db),
                                       callback=self.parse_reviews)
                r.meta['wid'] = wid
                yield r

    def parse_reviews(self, response):
        for review_sel in response.xpath(self.xpath_reviews):
            sel1 = review_sel.xpath(self.xpath_rtext_rating)
            # TODO: issues when text review includes markup inline (text under markup is skipped)
            rtext = sel1.xpath('./text()').extract()[0]
            r_list = sel1.xpath('./span[@class="rating"]/img/@src').extract()  # gives list of [http://pics..../ss6.gif]
            if r_list or len(r_list) == 1:
                r = r_list[0]
                rating = r[r.rindex('pics/') + 5:]  # gives ss10.gif
            else:
                rating = None

            sel2 = review_sel.xpath(self.xpath_user_date)
            username = sel2.xpath('./a[starts-with(@href,"/profile/")]/@href').extract()[0]  # gives /profile/yermat
            rdate = sel2.xpath('./text()').extract()[0]  # gives :   |  Nov 22, 2012  |
            rawdate = rdate[rdate.index('|') + 1:rdate.rindex('|')].strip()

            item = self.build_review_item()
            item['work_uid'] = response.meta['wid']
            item['username'] = username[username.rindex('/') + 1:]
            item['rating'] = rating
            item['review'] = rtext
            item['review_date'] = rawdate
            yield item

    def scrape_langs_nb(self, response):
        """Extract language/nb of reviews (assuming English only when lang bar menu not found)
        (TODO: fix-this assumption later... some book only have reviews in one foreign lang)
        :return {'English':  34, 'French': 12, .. }
        """
        lang_codes_nb = {}
        list_l_n = response.xpath(self.langs_root).extract()
        if len(list_l_n) == 0:
            # 'Showing 4 of 4'
            show_txt = response.xpath('//div[@id="mainreviews_reviewnav"]/text()').extract()[0]
            nb = int(show_txt[show_txt.rindex('of') + 2:])
            lang_codes_nb['English'] = nb
        else:
            for i in xrange(len(list_l_n)):
                if list_l_n[i].find('(') != -1:
                    nb = list_l_n[i]
                    lang_codes_nb[list_l_n[i - 1]] = int(nb[nb.index('(') + 1:nb.index(')')])

        if len(list_l_n) == 1:
            # to log
            print("The page '%s' has language bar but only one lang" % response.ur)

        if u'All languages' in lang_codes_nb:
            lang_codes_nb.pop(u'All languages')
        return lang_codes_nb

    def parse_review_date(self, raw_date):
        return datetime.strptime(raw_date, '%b %d, %Y')


class CritiquesLibresReview(BaseReviewSpider):
    """
    First Step: Fetch # of Reviews in Json format using request:
        # http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=0&limit=300
        (adjust: start=? and limit=?)

        Json :  {"total":"44489", "data":[ {"id":"", "titre":"", "nbrcrit":"", ...}, {"id":....}] }
        where total is the total number of books reviewed.

    Second step:  Fetch new reviews ready for loading.

    Note:
    1) critique can be modified during 7 days, so Spider MUST BE RUN after 7 days after end-of-period !!
    2) takes only 4 min to harvest all reviews (period 2000-2014), so can be lauched in a single session
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


    def __init__(self, **kwargs):
        # don't use self.name as instance variable could shadow the static one (to confirm?)
        super(CritiquesLibresReview, self).__init__(CritiquesLibresReview.name, **kwargs)

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
            if nreviews_eclair > 1:  # TODO: implement same logic as other with works_to_harvest instead of self.lookup_stored_nb_reviews(bookuid):
                lname, fname = self.parse_author(review['auteurstring'])
                item = ReviewItem(hostname=self.allowed_domains[0],
                                  site_logical_name=self.name,
                                  book_uid=bookuid,
                                  book_title=review['titre'],
                                  book_lang=self.lang,
                                  author_fname=fname,
                                  author_lname=lname)
                # trigger the 2nd Request
                request = scrapy.Request(self.url_review % int(bookuid), callback=self.parse_review)
                request.meta['item'] = item
                yield request

    def parse_review(self, response):
        passed_item = response.meta['item']

        if resolve_value(response.selector, self.xpath_title) != passed_item['book_title']:
            raise ValueError("Book title in webpage ('%s') different from Json ('%s')"
                             % (
                             resolve_value(response.selector, self.xpath_title), response.meta['item']['book_title']))

        allreviews = response.xpath(self.xpath_allreviews)
        rowno = 1
        for review_sel in allreviews:
            # iterate through 3 rows for each critics : row-1: title_star, row-2: username + date, row-3: horizontal line
            if rowno == 1:
                passed_item['rating'] = resolve_value(review_sel, self.xpath_rating)
                # TODO: add the review which is now the review content as text
                rowno = 2
            elif rowno == 2:
                ruid = resolve_value(review_sel, self.xpath_reviewer_uid)
                rdate = resolve_value(review_sel, self.xpath_date)
                passed_item['username'] = resolve_value(review_sel, self.xpath_username),
                passed_item['user_uid'] = ruid[ruid.rfind("=") + 1:],
                passed_item['review_date'] = rdate[rdate.rfind("-") + 2:]
                rowno = 3
            else:
                rowno = 1
                yield passed_item

    def parse_review_date(self, review_date_str):
        month_name = review_date_str[(review_date_str.find(' ') + 1): review_date_str.rfind(' ')]
        month_no = scrapy_utils.mois[month_name]
        review_date_str = review_date_str.replace(month_name, month_no)
        return datetime.strptime(review_date_str, '%d %m %Y')

    def parse_author(self, author_str):
        i = author_str.index(', ')
        lname = author_str[0:i]
        fname = author_str[i + 2:]
        return (lname, fname)


class BabelioSpider(BaseReviewSpider):
    """
    Babelio has no global list to easily crawl for total #ofReviews.  Best approach is to
    use ISBN from persisted reviews and search reviews based on these.
    As a consequence, only reviews from already persisted book are scraped from this site.
    """
    name = 'babelio'
    allowed_domains = ['www.babelio.com']

    # Book_uid is defined in this site as 'title/id' (ex. 'Green-Nos-etoiles-contraires/436732'
    # tri=dt order by date descending
    review_url_param = "http://www.babelio.com/livres/%s/critiques?pageN=2&tri=dt"

    def __init__(self, **kwargs):
        super(BabelioSpider, self).__init__(BabelioSpider.name, **kwargs)
        pass

    def start_requests(self):
        pass
        # yield scrapy.Request(self.url_nb_reviews % (0, 2), callback=self.init_max_nb_items)


class DecitreSpider(BaseReviewSpider):
    """
    This is similar to Babelio, only a search can be easily implemented
    """
    pass


class WorkReference(BaseAuditSpider):

    name = 'workreference'
    allowed_domains = ['www.librarything.com']

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
        item = Item(work_uid=wid,
                    title=title,
                    original_lang=ori_lang,
                    authors_name=authors_name,
                    authors_disamb_id=authors_id,
                    lc_subjects=6,
                    ddc_mds=7,
                    load_audit_id=8)

        yield item
