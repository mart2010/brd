# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class ReviewBaseItem(Item):
    """Make sure these match with staging.review table, since
    we rely on these name for building insert statement
    """
    hostname = Field()
    reviewer_pseudo = Field()
    reviewer_uid = Field()
    review_rating = Field()
    review_date = Field()
    review_text = Field()

    book_isbn = Field()
    book_title = Field()
    book_uid = Field()
    derived_title_sform = Field()
    derived_review_date = Field()
    book_lang = Field()




class BookBaseItem(Item):
    pass



