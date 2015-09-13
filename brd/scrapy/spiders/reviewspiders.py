# coding=utf-8

__author__ = 'mouellet'

import json

import scrapy
import scrapy.xlib.pydispatch.dispatcher as dispatcher
from scrapy.loader import ItemLoader, Identity

from brd.scrapy.items import ReviewBaseItem
import brd.db.dbutils as dbutils
import brd.utils as utils


def resolve_value(selector, xpath, expected=1):
    """
    Return element values extracted from selector using xpath.
    :return: list of values or one value when expected=1 and list has one element.
    """
    val = selector.xpath(xpath).extract()
    if val is None:
        raise ValueError("Value extracted from selector '%s' with xpath: %s is None" % (selector, xpath))
    if hasattr(val, '__iter__'):
        if expected == len(val):
            return val[0] if expected == 1 else val
        else:
            raise ValueError("Expected %d elements, instead got: '%s' using selector '%s' with xpath '%s' " % (expected, val, selector, xpath))
    return val



class CritiquesLibresSpider(scrapy.Spider):
    """ Fetches first latest Reviews as Json objects returned from this simplified request:
    # http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=0&limit=300 (adjust: start=? and limit=?)

    Jsons returned:  {"total":"44489", "data":[ {"id":"", "titre":"", "nbrcrit":"", ...}, {"id":....}] }
    where total is the total number of books reviewed.  To parse json, remove first and last char '(' and ')'

    Second step is to fetch new reviews ready for loading.

    Note: critique est modifiable pendant sept jours minimum (plus si pas de critique plus recente),
         so we need to load critic based on the period load dates:  period_begin <= critic_date+7 < period_end
    """
    name = 'critiqueslibres'
    # Important: used also for setting 'hostname' item field!
    allowed_domains = ['www.critiqueslibres.com']

    start_url_param = "http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=%d&limit=%d"
    # Sometime the '?alt=print' is delayed compared to main page (should be ok as we load critic at least 7 days old...)
    review_url_param = "http://www.critiqueslibres.com/i.php/vcrit/%d?alt=print"

    items_per_page = 200
    # to find dynamically
    max_nb_items = 2000
    min_nb_reviews = 5

    # used to know when new reviews need to be loaded
    # {bookid : #nbReviews}
    nbreviews_stored = {45058: 1, 45032: 2}

    # see how to pass-in param for start-date/end-date (loading date period)

    # Xpath attributes for Review page (find better/stable xpath)
    allreviews_root = '//table[@width="100%" and @border="0" and @cellpadding="3"]/tr'
    # Main-page allreviews_root = '//td[@class="texte titre" and starts-with(.,"Les critiques éclairs")]/parent::*/following-sibling::*'

    book_title      = '//td[@class="texte"]/p/strong/text()'
    review_rating   = './td[@class="texte"]/img[contains(@name,"etoiles")]/@name'
    # Main-page review_rating   = './/img[contains(@name,"etoiles")]/@name'
    review_date     = './td[@class="texte"]/p[2]/text()'     # return:  " - BOURGES - 50 ans - 1 décembre 2004"
    reviewer_pseudo = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/text()'
    reviewer_uuid   = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/@href'   # return: "/i.php/vuser/?uid=32wdqee2334"


    def start_requests(self):
        for i in range(0, self.max_nb_items, self.items_per_page):
            yield scrapy.Request(self.start_url_param % (i, self.items_per_page), callback=self.parse)

    def parse(self, response):
        pageres = json.loads(response.body[1:-1])

        if pageres['success'] != 0:
            raise IOError("Page result error for request: " + response.request)

        for review in pageres['data']:
            # Only reviews-eclaire considered for scraping (i.e. = #reviews - 1 )
            nreviews_eclair = int(review['nbrcrit']) - 1
            bookid = review['id']
            # only scrape the ones having enough review-eclair (nbreviews)
            if nreviews_eclair >= self.min_nb_reviews and nreviews_eclair > int(self.nbreviews_stored.get(bookid, 0)):
                item = ReviewBaseItem()
                item['hostname'] = self.allowed_domains[0]
                item['book_uid'] = bookid
                # trigger the second request
                request = scrapy.Request(self.review_url_param % int(bookid), callback=self.parse_review)
                request.meta['item'] = item
                yield request

    def parse_review(self, response):
        book_title = resolve_value(response.selector, self.book_title)
        allreviews = response.xpath(self.allreviews_root)
        rowno = 1
        for review_sel in allreviews:
            # iterate through 3 rows for each critics : row-1: title_star, row-2: pseudo + date, row-3: horizontal line
            if rowno == 1:
                item = response.meta['item']
                item['review_rating'] = resolve_value(review_sel, self.review_rating)
                rowno = 2
            elif rowno == 2:
                item['reviewer_pseudo'] = resolve_value(review_sel, self.reviewer_pseudo)
                ruid = resolve_value(review_sel, self.reviewer_uuid)
                item['reviewer_uid'] = ruid[ruid.rfind("=") + 1:]
                rdate = resolve_value(review_sel, self.review_date)
                item['review_date'] = rdate[rdate.rfind("-")+2:]
                rowno = 3
            else:
                # horizontal line, finish processing the item
                item['book_title'] = book_title
                item['derived_title_sform'] = utils.convert_to_sform(book_title)
                rowno = 1
                yield item


    def __init__(self):

        # much simpler to call directly the init here... (see if better prev solution for diff runtimes scenario)
        self.init_nbreviews_stored()


    # def spider_opened(self, spider):
    #     # maybe not be true when running off from the engine with many different spiders
    #     assert(self is spider, "Expecting to receive self as spider, otherwise chnage code accordingly..")
    #     self.init_nbreviews_stored()


    def init_nbreviews_stored(self):

        squery = """select book_uid, nb_reviews
                    from integration.critiqueslibres_lookup
                 """
        res = dbutils.get_ro_connection().execute_transaction(squery)
        if res is not None:
            for row in dbutils.get_ro_connection().execute_transaction(squery):
                self.nbreviews_stored[row[0]] = row[1]




