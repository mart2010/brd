# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class ReviewItem(Item):
    """Item names must match staging.review column names (not anymore)
    """
    site_logical_name   = Field()
    username            = Field()
    user_uid            = Field()
    rating              = Field()
    review              = Field()
    review_date         = Field()
    review_lang         = Field()
    likes               = Field()
    work_uid            = Field()
    dup_uid             = Field()
    parsed_rating       = Field()
    book_title          = Field()
    book_author         = Field()
    parsed_review_date  = Field()
    parsed_rating       = Field()


class WorkItem(Item):
    work_uid            = Field()
    title               = Field()
    original_lang       = Field()
    authors_name        = Field()
    authors_disamb_id   = Field()
    lc_subjects         = Field()
    ddc_mds             = Field()
    load_audit_id       = Field()


class ReviewerItem(Item):
    pass



