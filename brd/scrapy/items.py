# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class ReviewItem(Item):
    """Item names must match staging.review column names
    """
    site_logical_name   = Field()
    username            = Field()
    user_uid            = Field()
    rating              = Field()
    review              = Field()
    review_date         = Field()
    review_lang         = Field()
    likes               = Field()
    work_refid          = Field()
    dup_refid           = Field()
    work_uid            = Field()
    parsed_review_date  = Field()
    parsed_rating       = Field()


class WorkRefItem(Item):
    work_refid          = Field()
    dup_refid           = Field()
    title               = Field()
    authors             = Field()
    authors_code        = Field()
    original_lang       = Field()
    ori_lang_code       = Field()
    lc_subjects         = Field()
    mds_code            = Field()
    mds_text            = Field()
    popularity          = Field()


class ReviewerItem(Item):
    pass



