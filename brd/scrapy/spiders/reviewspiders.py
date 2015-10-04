# -*- coding: utf-8 -*-

from brd.scrapy.utils import resolve_value
import re

__author__ = 'mouellet'

import json
import scrapy
from brd.scrapy.items import ReviewBaseItem
import brd.scrapy.utils as utils
from datetime import datetime
import string


def populate_item(item, **kitems):
    for key in kitems.iterkeys():
        item[key] = kitems[key]


# ------------------------------------------------------------------------------------------------- #
#                                        ReviewBaseSpider                                           #
# ------------------------------------------------------------------------------------------------- #

class BaseReviewSpider(scrapy.Spider):

    # business-rules
    min_nb_reviews = 5


    def __init__(self, logical_name, **kwargs):
        """
        Make sure all mandatory arguments are passed to all Spider subclass
        (ex. scrapy crawl myspider -a param1=val1 -a param2=val2)
        :param begin_period: Review's minimum date to scrape ('dd/mm/yyyy')
        :param end_period: Review's maximum date (exclusive) to scrape ('dd/mm/yyyy')
        :param logical_name: name defined in the review subclass scraper (used as a lookup)
        """
        super(BaseReviewSpider, self).__init__(**kwargs)

        if not kwargs.has_key('begin_period'):
            kwargs['begin_period'] = '01/01/2000'
            kwargs['end_period'] = '01/01/2010'

        # assert(kwargs['begin_period'] is not None)
        # assert(kwargs['end_period'] is not None)

        self.begin_period = datetime.strptime(kwargs['begin_period'], '%d/%m/%Y')
        self.end_period = datetime.strptime(kwargs['end_period'], '%d/%m/%Y')

        # must match site.logical_name at DB level
        self.logical_name = logical_name

        # dict to hold the nb-reviews already persisted: {"book_uid": "nb"}
        self.stored_nb_reviews = utils.fetch_nbreviews(self.logical_name)


    def parse(self, response):
        pass


# ------------------------------------------------------------------------------------------------- #
#                                            ReviewSpider                                           #
# ------------------------------------------------------------------------------------------------- #

class CritiquesLibresSpider(BaseReviewSpider):
    """ First Step is to fetch latest Reviews in Json format using request:
    # http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=0&limit=300 (adjust: start=? and limit=?)

    Json :  {"total":"44489", "data":[ {"id":"", "titre":"", "nbrcrit":"", ...}, {"id":....}] }
    where total is the total number of books reviewed.

    Second step is to fetch new reviews ready for loading.

    Note: critique est modifiable pendant sept jours minimum (plus si pas de critique plus recente),
         so we need to load critic basnb_review = int( nb_text[0:nb_text.index(' ')] )ed on the period load dates:  period_begin <= critic_date+7 < period_end
    """
    name = 'critiqueslibres'
    allowed_domains = ['www.critiqueslibres.com']  # Important: also used to set 'hostname' item field


    def __init__(self, **kwargs):
        # here don't use self.name as it would create an instance variable and shadow the static one (to confirm?)
        super(CritiquesLibresSpider, self).__init__(CritiquesLibresSpider.name, **kwargs)

    url_nb_reviews = "http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=%d&limit=%d"
    items_per_page = 200
    # TODO: find dynamically
    max_nb_items = 2000

    def start_requests(self):
        for i in range(0, self.max_nb_items, self.items_per_page):
            yield scrapy.Request(self.url_nb_reviews % (i, self.items_per_page), callback=self.parse_nb_reviews)

    # ('?alt=print' may be delayed compared to main page, ok if load critic at least 7 days old...)
    url_review = "http://www.critiqueslibres.com/i.php/vcrit/%d?alt=print"
    # Xpath attributes for Review page (find better/stable xpath)
    xpath_allreviews = '//table[@width="100%" and @border="0" and @cellpadding="3"]/tr'

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
                item = ReviewBaseItem()
                populate_item(item, hostname=self.allowed_domains[0], book_uid=bookid, book_title=review['titre'])
                # trigger the 2nd Request
                request = scrapy.Request(self.url_review % int(bookid), callback=self.parse_review)
                request.meta['item'] = item
                yield request


    xpath_title    = '//td[@class="texte"]/p/strong/text()'
    xpath_rating   = './td[@class="texte"]/img[contains(@name,"etoiles")]/@name'
    xpath_date     = './td[@class="texte"]/p[2]/text()'     # return:  " - BOURGES - 50 ans - 1 décembre 2004"
    xpath_pseudo   = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/text()'
    xpath_reviewer_uid  = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/@href'   # return: "/i.php/vuser/?uid=32wdqee2334"

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
                new_item = ReviewBaseItem()
                populate_item(new_item, hostname=passed_item['hostname'], book_uid=passed_item['book_uid'],
                              book_title=passed_item['book_title'], review_rating=resolve_value(review_sel, self.xpath_rating) )
                rowno = 2
            elif rowno == 2:
                ruid = resolve_value(review_sel, self.xpath_reviewer_uid)
                rdate = resolve_value(review_sel, self.xpath_date)
                populate_item(new_item, reviewer_pseudo=resolve_value(review_sel, self.xpath_pseudo),
                              reviewer_uid=ruid[ruid.rfind("=") + 1:], review_date=rdate[rdate.rfind("-") + 2:])
                rowno = 3
            else:
                rowno = 1
                yield new_item

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
        # for stored_






# ------------------------------------------------------------------------------------------------- #
#                                            ReviewSpider                                           #
# ------------------------------------------------------------------------------------------------- #
class GuidelectureSpider(BaseReviewSpider):
    """
    Very similar pattern than critiqueslibres, should be merged into a parent class with children on
    overriding custom xpath...or using composition instead of inheritance...

    HOWEVER, this site is quire dated, has few reviews and some duplicates and data issues
    DEV NOT YET FINISHED!
    """
    name = 'guidelecture'
    allowed_domains = ['www.guidelecture.com']  # Important: also used to set 'hostname' item field


    def __init__(self, **kwargs):
        super(GuidelectureSpider, self).__init__(GuidelectureSpider.name, **kwargs)


    # 1st Step url: liste de critiques
    # param: titre: A, B, ... Z, 1, 2, .. 9
    url_nb_reviews = "http://www.guidelecture.com/titres.asp?page=%s"

    def start_requests(self):
        page_params = list(string.ascii_uppercase) + [str(i) for i in range(1, 10)]
        for p in page_params:
            yield scrapy.Request(self.url_nb_reviews % p, callback=self.parse_nb_reviews)


    # 2nd Step url
    url_review = "http://www.guidelecture.com/critiquet.asp?titre=%s"
    xpath_all_nb_reviews =  '//tr[@bgcolor="#FFFFFF" or @bgcolor="#EEEEEE"]'  # all_rows = '//p[@class="Titre"]/following-sibling::p[1]/table/tbody/tr' (only works in chrome tool)
    xpath_title = './td[1]/font/i/a/text()'
    # this give the number of reviews as list
    xpath_nb_review = './td[2]/font/text()'   # ex. [u' ']  or  [u'2 critiques ']


    def parse_nb_reviews(self, response):
        print "The response encoding is  " + str(response.encoding)

        for row in response.xpath(self.xpath_all_nb_reviews):
            nb_text = row.xpath(self.xpath_nb_review).extract()[0]

            if nb_text.find('critique') > -1:
                nb_review = int(nb_text[0:nb_text.index(' ')])
                # book title is used as book_uid
                book_uid = row.xpath(self.xpath_title).extract()[0]

                if self.min_nb_reviews <= nb_review > int(self.stored_nb_reviews.get(book_uid, 0)):
                    item = ReviewBaseItem()
                    populate_item(item, hostname=self.allowed_domains[0], book_uid=book_uid, book_title=book_uid)
                    # trigger the 2nd Request
                    # here instead of building the url exactly, rely on REquest object to define query with parameters.. to garentee encoding
                    request = scrapy.Request(self.url_review % book_uid, callback=self.parse_review)
                    request.meta['item'] = item
                    yield request


    def parse_review(self):
        pass


    article_repl = (u"(Aux)", u"(Au)", u"(Les)", u"(L')", u"(Le)", u"(La)", u"(Un)", u"(Une)", u"(Des)", u"(D')", u"(Mes)")

    def reformat_title(self, title):
        """
        >>> GuidelectureSpider().reformat_title(u"adieu aux armes (L')")
        u"L'adieu aux armes"
        >>> GuidelectureSpider().reformat_title(u"tapisserie de Fionavar (La) (serie)")
        u'La tapisserie de Fionavar  (serie)'
        """
        left_p = title.find(u'(')
        right_p = title.find(u')')

        if -1 < left_p < right_p:
            par = title[left_p:right_p + 1]
            # if article, put it back in front
            if par in self.article_repl:
                title = title.replace(par, u"")
                apost = par.find(u"'")
                title = par[1:-1] + title if apost > -1 else par[1:-1] + " " + title
        return title.strip()






