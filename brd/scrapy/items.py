# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class ReviewBaseItem(Item):

    reviewer_pseudo = Field()
    reviewer_uuid = Field()    #optional
    reviewer_name = Field()    #optional

    book_title = Field()
    book_isbn = Field()
    book_author = Field()
    book_lang = Field()


    review_rating = Field()
    review_date = Field()
    review_text = Field()
    review_url = Field()    #handy for increment updating..




class BookBaseItem(scrapy.Item):
    pass



