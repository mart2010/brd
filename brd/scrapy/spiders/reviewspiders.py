__author__ = 'mouellet'

import scrapy
from scrapy.loader import ItemLoader, Identity
from brd.scrapy.items import ReviewBaseItem
import json

# missing datetime of the review, url?,

# TODO: should write an Input-processor that would raise Exception in case multiple values are returned
# ex. I do not expect more than one reviewer_pseudo for each call to loader.xpath()...



class CritiquesLibresSpider(scrapy.Spider):
    """ Spider to fetch latest stats (#ofReviews) as Json objects
    # Json object is returned (php) from this simplified request:
    # http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=0&limit=300
    # Must adjust start=? and limit=?
    # Jsons returned:  {"total":"44489", "data":[ {"id":"bookId", "titre":"dddd", ...}, {"id":....}] }
    # where total is the total number of books reviewed.
    # To parse json, remove first and last char '(' and ')'  ustrin = response.body_as_unicode()[1:-1]
    """
    name = 'critiqueslibres'
    allowed_domains = ['www.critiqueslibres.com']

    start_url_param = "http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=%d&limit=%d"
    review_url_param = "http://www.critiqueslibres.com/i.php/vcrit/%d?alt=print"

    items_per_page = 200
    # to find dynamically
    max_nb_items = 45000


    #intercept the open crawler callback event (if this is possible) to fetch latest stats
    # {bookid : #nbReviews}
    nb_persisted_reviews = {45058: 1, 45032: 2}

    #see how to pass-in param for start-date/end-date (loading date period)

    # Xpath attributes for Review page
    allreviews_root = '//table/tr'

    reviewer_pseudo = './td[@class="texte"]/p/a[contains(@href,"uid=")]/text()'
    reviewer_uuid   = './td[@class="texte"]/p/a[contains(@href,"uid=")]/@href'
    book_title      = './td[@class="titre"]/text()'
    review_rating   = './td[@class="texte"]/img[contains(@name,"etoiles")]/@name'
    review_date     = ''


    def start_requests(self):
        #for i in range(0, self.max_nb_items, step=self.items_per_page):
        i = 1000
        yield scrapy.Request(self.start_url_param % (i, self.items_per_page), callback=self.parse)

    def parse(self, response):
        pageres = json.loads(response.body_as_unicode()[1:-1])

        if pageres['success'] != 0:
            raise IOError("Page result error for request: " + response.request)

        for review in pageres['data']:
            nbreviews = int(review['nbrcrit'])
            bookid = int(review['id'])
            if nbreviews > 1 and nbreviews > (self.nb_persisted_reviews[bookid]):
                # trigger a second request
                yield scrapy.Request(self.review_url_param % (bookid), callback=self.parse_review )

    def parse_review(self, response):
        allreviews = response.xpath(self.allreviews_root)
        for review in allreviews:
            # Use a simple loader and built-in processor (default_input_processor= Identity(), default_output_processor = Identity())
            loader = ItemLoader(item=ReviewBaseItem(), selector=review)
            # see if possible to reuse and refer to naming : test self.book_title.__str__() ??
            loader.add_xpath('reviewer_pseudo', self.reviewer_pseudo)
            loader.add_xpath('reviewer_uuid', self.reviewer_uuid)
            loader.add_xpath('book_title', self.book_title)
            loader.add_xpath('review_rating', self.review_rating)

            yield loader.load_item()






