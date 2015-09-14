# coding=utf-8
from brd.scrapy.utils import resolve_value

__author__ = 'mouellet'

import json

import scrapy
from brd.scrapy.items import ReviewBaseItem
import brd.scrapy.utils as utils
from datetime import datetime



def populate_item(item, **kitems):
    for key in kitems.iterkeys():
        item[key] = kitems[key]


class BaseSpider(scrapy.Spider):

    name = 'To-Override'
    # Important: this is also used for setting 'hostname' item field
    allowed_domains = ['To-Override']

    # Used to know when new reviews ready to be scraped  {bookid : #nbReviews}
    nbreviews_stored = {}

    def __init__(self, **kwargs):
        """
        Make sure all mandatory arguments are passed to all Spider subclass
        (ex. scrapy crawl myspider -a param1=val1 -a param2=val2)
        :param begin_period: Review's minimum date to scrape ('dd/mm/yyyy')
        :param end_period: Review's maximum date (exclusive) to scrape ('dd/mm/yyyy')
        """
        assert(kwargs['begin_period'] is not None)
        assert(kwargs['end_period'] is not None)
        self.begin_period = datetime.strptime(kwargs['begin_period'], '%d/%m/%Y')
        self.end_period = datetime.strptime(kwargs['end_period'], '%d/%m/%Y')
        super(BaseSpider, self).__init__(**kwargs)


class CritiquesLibresSpider(BaseSpider):
    """ First Step is to fetch latest Reviews in Json format using request:
    # http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=0&limit=300 (adjust: start=? and limit=?)

    Json :  {"total":"44489", "data":[ {"id":"", "titre":"", "nbrcrit":"", ...}, {"id":....}] }
    where total is the total number of books reviewed.

    Second step is to fetch new reviews ready for loading.

    Note: critique est modifiable pendant sept jours minimum (plus si pas de critique plus recente),
         so we need to load critic based on the period load dates:  period_begin <= critic_date+7 < period_end
    """
    name = 'critiqueslibres'
    allowed_domains = ['www.critiqueslibres.com']

    start_url_param = "http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=%d&limit=%d"
    # Sometime the '?alt=print' is delayed compared to main page (should be ok as we load critic at least 7 days old...)
    review_url_param = "http://www.critiqueslibres.com/i.php/vcrit/%d?alt=print"

    items_per_page = 200
    # TODO: to find dynamically
    max_nb_items = 2000
    min_nb_reviews = 5


    # Xpath attributes for Review page (find better/stable xpath)
    allreviews_root = '//table[@width="100%" and @border="0" and @cellpadding="3"]/tr'
    # Main-page allreviews_root = '//td[@class="texte titre" and starts-with(.,"Les critiques éclairs")]/parent::*/following-sibling::*'

    book_title      = '//td[@class="texte"]/p/strong/text()'
    review_rating   = './td[@class="texte"]/img[contains(@name,"etoiles")]/@name'
    review_date     = './td[@class="texte"]/p[2]/text()'     # return:  " - BOURGES - 50 ans - 1 décembre 2004"
    reviewer_pseudo = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/text()'
    reviewer_uuid   = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/@href'   # return: "/i.php/vuser/?uid=32wdqee2334"


    def __init__(self, **kwargs):
        super(CritiquesLibresSpider, self).__init__(**kwargs)
        self.nbreviews_stored = utils.fetch_nbreviews_stored(self.name)


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
            # only scrape the ones having enough review-eclair (nbreviews) and not yet stored
            if self.min_nb_reviews <= nreviews_eclair > int(self.nbreviews_stored.get(bookid, 0)):
                item = ReviewBaseItem()
                populate_item(item, hostname=self.allowed_domains[0], book_uid=bookid, book_title=review['titre'])
                # trigger the 2nd Request
                request = scrapy.Request(self.review_url_param % int(bookid), callback=self.parse_review)
                request.meta['item'] = item
                yield request


    def parse_review(self, response):
        passed_item = response.meta['item']

        if resolve_value(response.selector, self.book_title) != passed_item['book_title']:
            raise ValueError("Book title in webpage ('%s') different from Json ('%s')"
                             % (resolve_value(response.selector, self.book_title), response.meta['item']['book_title']))

        allreviews = response.xpath(self.allreviews_root)
        rowno = 1
        for review_sel in allreviews:
            # iterate through 3 rows for each critics : row-1: title_star, row-2: pseudo + date, row-3: horizontal line
            if rowno == 1:
                new_item = ReviewBaseItem()
                populate_item(new_item, hostname=passed_item['hostname'], book_uid=passed_item['book_uid'],
                              book_title=passed_item['book_title'], review_rating=resolve_value(review_sel, self.review_rating) )
                rowno = 2
            elif rowno == 2:
                ruid = resolve_value(review_sel, self.reviewer_uuid)
                rdate = resolve_value(review_sel, self.review_date)
                populate_item(new_item, reviewer_pseudo=resolve_value(review_sel, self.reviewer_pseudo),
                              reviewer_uid=ruid[ruid.rfind("=") + 1:], review_date=rdate[rdate.rfind("-") + 2:])
                rowno = 3
            else:
                rowno = 1
                yield new_item

    def parse_review_date(self, review_date):
        month_name = review_date[(review_date.find(' ') + 1): review_date.rfind(' ')]
        month_no = utils.mois[month_name]
        review_date = review_date.replace(month_name, month_no)
        return datetime.strptime(review_date, '%d %m %Y')


