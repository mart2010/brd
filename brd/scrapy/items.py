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
    site_logical_name   = Field()
    username            = Field()
    user_uid            = Field()
    work_uid            = Field()
    rating              = Field()
    parsed_rating       = Field()
    review              = Field()
    likes               = Field()
    review_date         = Field()
    parsed_review_date  = Field()


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



