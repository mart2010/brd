# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class ReviewItem(Item):
    """Item names must match staging.review column names
    (export stmt is constructed automatically based on that)
    """
    hostname = Field()
    site_logical_name = Field()
    reviewer_pseudo = Field()
    reviewer_uid = Field()
    review_rating = Field()
    review_date = Field()
    review_text = Field()

    book_isbn_list = Field()
    book_title = Field()
    book_uid = Field()
    book_lang = Field()

    author_fname = Field()
    author_lname = Field()

    parsed_review_date = Field()
    load_audit_id = Field()



class BookItem(Item):
    pass

class ReviewerItem(Item):
    pass



