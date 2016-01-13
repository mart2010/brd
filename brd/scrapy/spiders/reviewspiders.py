# -*- coding: utf-8 -*-
import brd

__author__ = 'mouellet'

from brd.scrapy.scrapy_utils import resolve_value
import json
import scrapy
from brd.scrapy.items import ReviewItem
import brd.scrapy.scrapy_utils as scrapy_utils
from datetime import datetime
import brd.config as config



# ------------------------------------------------------------------------------------------------- #
#                                        ReviewBaseSpider                                           #
# ------------------------------------------------------------------------------------------------- #

class BaseReviewSpider(scrapy.Spider):

    def __init__(self, logical_name, **kwargs):
        """
        Make sure all arguments are passed to all Spider subclass
        (ex. scrapy crawl myspider -a param1=val1 -a param2=val2).
        : period: Review's minimum/max (exclusive) period to scrape as 'd-m-yyyy_d-m-yyyy'
        : logical_name: name defined in the review subclass scraper (static name field)
                (must match site.logical_name at DB level)
        """
        super(BaseReviewSpider, self).__init__(**kwargs)
        assert (kwargs['period'] is not None)
        self.logical_name = logical_name
        self.begin_period, self.end_period = brd.resolve_period_text(kwargs['period'])

        # dict to hold nb-reviews persisted: {"book_uid": "nb"}  (check scalability)
        self.stored_nb_reviews = None

    def parse_review_date(self, review_date_str):
        raise NotImplementedError("method to be implementes by sub-class")

    def lookup_stored_nb_reviews(self, bookuid):
        """
        :return nb of reviews persisted for bookid (or 0 if not found in DB)
        """
        if self.stored_nb_reviews is None:
            self.stored_nb_reviews = scrapy_utils.fetch_nbreviews(self.logical_name)
        return int(self.stored_nb_reviews.get(bookuid, 0))



# ------------------------------------------------------------------------------------------------- #
#                                            ReviewSpider                                           #
# ------------------------------------------------------------------------------------------------- #

class CritiquesLibresSpider(BaseReviewSpider):
    """
    First Step: Fetch latest Reviews in Json format using request:
        # http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=0&limit=300
        (adjust: start=? and limit=?)

        Json :  {"total":"44489", "data":[ {"id":"", "titre":"", "nbrcrit":"", ...}, {"id":....}] }
        where total is the total number of books reviewed.

    Second step:  Fetch new reviews ready for loading.

    Note: critique can be modified during 7 days, so Spider MUST BE RUN after 7 days after end-of-period !!
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
        # don't use self.name as instance variable that could shadow the static one (to confirm?)
        super(CritiquesLibresSpider, self).__init__(CritiquesLibresSpider.name, **kwargs)


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
            # only crawls reviews having min reviews required and not yet persisted
            if config.MIN_NB_REVIEWS <= nreviews_eclair > self.lookup_stored_nb_reviews(bookuid):
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
                             % (resolve_value(response.selector, self.xpath_title), response.meta['item']['book_title']))

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



# ------------------------------------------------------------------------------------------------- #
#                                            ReviewSpider                                           #
# ------------------------------------------------------------------------------------------------- #

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


