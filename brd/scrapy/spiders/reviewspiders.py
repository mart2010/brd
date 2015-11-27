# -*- coding: utf-8 -*-
__author__ = 'mouellet'

from brd.scrapy.utils import resolve_value
import json
import scrapy
from brd.scrapy.items import ReviewBaseItem
import brd.scrapy.utils as utils
from datetime import datetime
import brd.config as config



# ------------------------------------------------------------------------------------------------- #
#                                        ReviewBaseSpider                                           #
# ------------------------------------------------------------------------------------------------- #

class BaseReviewSpider(scrapy.Spider):
    # business-rules
    min_nb_reviews = 5

    def __init__(self, logical_name, **kwargs):
        """
        Make sure all mandatory arguments are passed to all Spider subclass
        (ex. scrapy crawl myspider -a param1=val1 -a param2=val2).
        :begin_period: Review's minimum date to scrape ('d-m-yyyy')
        :end_period: Review's maximum date (exclusive) to scrape ('d-m-yyyy')
        :logical_name: name defined in the review subclass scraper (static name field)
                (must match site.logical_name at DB level)
        """
        super(BaseReviewSpider, self).__init__(**kwargs)

        assert (kwargs['begin_period'] is not None)
        assert (kwargs['end_period'] is not None)

        self.logical_name = logical_name
        self.begin_period = datetime.strptime(kwargs['begin_period'], '%d-%m-%Y')
        self.end_period = datetime.strptime(kwargs['end_period'], '%d-%m-%Y')

        # dict to hold the nb-reviews already persisted: {"book_uid": "nb"}  (Probably not scalable and not to be used for all spider)
        # self.stored_nb_reviews = utils.fetch_nbreviews(self.logical_name)


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
    name = 'ReviewOfcritiqueslibres'
    allowed_domains = ['www.critiqueslibres.com']  # used as 'hostname' in item field
    lang = 'FR'

    ###########################
    # Control setting
    ###########################
    items_per_page = 200
    # TODO: find dynamically
    max_nb_items = 2000
    ##########################

    ###########################
    # Parse_nb_reviews setting
    ###########################
    url_nb_reviews = "http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=%d&limit=%d"
    xpath_allreviews = '//table[@width="100%" and @border="0" and @cellpadding="3"]/tr'  # for Review page (find better/stable xpath)
    ##########################

    ###########################
    # Parse_review setting
    ###########################
    url_review = "http://www.critiqueslibres.com/i.php/vcrit/%d?alt=print"
    xpath_title = '//td[@class="texte"]/p/strong/text()'
    xpath_rating = './td[@class="texte"]/img[contains(@name,"etoiles")]/@name'
    xpath_date = './td[@class="texte"]/p[2]/text()'  # return:  " - BOURGES - 50 ans - 1 décembre 2004"
    xpath_pseudo = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/text()'
    xpath_reviewer_uid = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/@href'  # return: "/i.php/vuser/?uid=32wdqee2334"
    ###########################

    def __init__(self, **kwargs):
        # don't use self.name as instance variable that could shadow the static one (to confirm?)
        super(CritiquesLibresSpider, self).__init__(self.name, **kwargs)

    def start_requests(self):
        for i in range(0, self.max_nb_items, self.items_per_page):
            yield scrapy.Request(self.url_nb_reviews % (i, self.items_per_page), callback=self.parse_nb_reviews)

    def parse_nb_reviews(self, response):
        pageres = json.loads(response.body[1:-1])

        if pageres['success'] != 0:
            raise IOError("Page result error for request: " + response.request)

        for review in pageres['data']:
            # Only reviews-eclaire considered for scraping (i.e. = #reviews - 1 )
            nreviews_eclair = int(review['nbrcrit']) - 1
            bookid = review['id']
            # only scrape the ones having enough review-eclair (nbreviews) and not yet stored
            if self.min_nb_reviews <= nreviews_eclair > int(self.stored_nb_reviews.get(bookid, 0)):
                item = ReviewBaseItem(hostname=self.allowed_domains[0],
                                      site_logical_name=self.name,
                                      book_uid=bookid,
                                      book_title=review['titre'],
                                      book_lang=self.lang)
                # trigger the 2nd Request
                request = scrapy.Request(self.url_review % int(bookid), callback=self.parse_review)
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
            # iterate through 3 rows for each critics : row-1: title_star, row-2: pseudo + date, row-3: horizontal line
            if rowno == 1:
                passed_item['review_rating'] = resolve_value(review_sel, self.xpath_rating)
                rowno = 2
            elif rowno == 2:
                ruid = resolve_value(review_sel, self.xpath_reviewer_uid)
                rdate = resolve_value(review_sel, self.xpath_date)
                passed_item['reviewer_pseudo'] = resolve_value(review_sel, self.xpath_pseudo),
                passed_item['reviewer_uid'] = ruid[ruid.rfind("=") + 1:],
                passed_item['review_date'] = rdate[rdate.rfind("-") + 2:]
                rowno = 3
            else:
                rowno = 1
                yield passed_item

    @staticmethod
    def parse_review_date(review_date):
        month_name = review_date[(review_date.find(' ') + 1): review_date.rfind(' ')]
        month_no = utils.mois[month_name]
        review_date = review_date.replace(month_name, month_no)
        return datetime.strptime(review_date, '%d %m %Y')


# ------------------------------------------------------------------------------------------------- #
#                                            ReviewSpider                                           #
# ------------------------------------------------------------------------------------------------- #
class BabelioSpider(BaseReviewSpider):
    """
    Babelio has a lot of critics but no global list to easily crawl for total #ofReviews.
    Strategy is to use titles from review loaded from other sites and trigger a search and follow
    link result to load the review.
    Or lookup review based on Book and sorted by Date!

    """
    name = 'Review_babelio'
    allowed_domains = ['www.babelio.com']

    # Book_uid is defined in this site as 'title/id' (ex. 'Green-Nos-etoiles-contraires/436732'
    # tri=dt order by date descending
    review_url_param = "http://www.babelio.com/livres/%s/critiques?pageN=2&tri=dt"

    def __init__(self, **kwargs):
        super(BabelioSpider, self).__init__(BabelioSpider.name, **kwargs)
        pass

    def start_requests(self):
        pass
        # for stored_
