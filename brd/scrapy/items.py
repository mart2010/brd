# -*- coding: utf-8 -*-
__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The BRD Project"

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
    title               = Field()
    authors             = Field()
    tags_t              = Field()
    tags_n              = Field()
    tags_lang           = Field()
    parsed_review_date  = Field()
    parsed_rating       = Field()
    parsed_likes        = Field()
    parsed_dislikes     = Field()

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
    mds_code_corr       = Field()
    popularity          = Field()
    other_lang_title    = Field()

class ReviewerItem(Item):
    username            = Field()
    user_uid            = Field()
    name                = Field()
    status              = Field()
    ocupation           = Field()
    interest            = Field()
    fav_books           = Field()
    country             = Field()
    location            = Field()
    gender              = Field()
    birth_text          = Field()
    parsed_birthyear    = Field()
    tags                = Field()

class IsbnLangItem(Item):
    ean                 = Field()
    lang_code           = Field()

