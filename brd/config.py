# -*- coding: utf-8 -*-

DATABASE = {
    'host': 'localhost',
    'name': 'brd',
    'port': '5432',
    'user': 'brd',
    'pwd' : 'brd'
}


# website section (to be stored in DB...)

#for now, in french:
SOURCE_WEBSITES_FR = {
    'critiqueslibres': 'http://www.critiqueslibres.com',  # 45K livres, 100K critics
    'decitre': 'http://www.decitre.fr',
    'amazon': 'http://www.amazon.fr',  # amazon purchased goodreads, but their reviews are not planned to be integrated..
    'fnac': 'http://www.fnac.com',
    'babelio': 'http://www.babelio.com',
    'shelfari': 'http://www.shelfari.com',  # also an amazon co!!
    # these sites are quite outdated and/or limited in term of reviews
    'critique-livre': 'http://www.critique-livre.fr',  # only 750 reviews on novel (roman)
    'senscritique': 'http://www.senscritique.com',  # only a number of top-list are available
    'guidelecture': 'http://www.guidelecture.com'  # outdated
}


SOURCE_WEBSITES_EN = {
    'goodreads': 'http://www.goodreads.com',  # 10M reviews of 700k titles
    'librarything': 'http://www.librarything.com'  # 2.5M reviews of 1M works
}


REVIEW_PREFIX = "ReviewOf"
REVIEW_EXT = ".csv"

SCRAPED_OUTPUT_DIR = "/Users/mart/Temp/reviews"
# archive NOT under SCRAPED_OUTPUT_DIR as we recursively load review files from there
SCRAPED_ARCHIVE_DIR = "/Users/mart/Temp/reviews_archive"
REF_DATA_DIR = "/Users/mart/Temp/ref_data"
REF_ARCHIVE_DIR = "/Users/mart/Temp/ref_data/archive"


# business rules

# Minimum nb of reviews before loading book from website
MIN_NB_REVIEWS = 5



# trigger settings
    




# etc..
