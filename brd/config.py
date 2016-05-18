# -*- coding: utf-8 -*-
__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The BRD Project"


DATABASE = {
    'host': 'localhost',
    # 'host': '192.168.0.28',
    'database': 'brd',
    'port': '5432', #  '54355',
    'user': 'brd'  , 'password': 'brd'
}


# website section (to be stored in DB...)

#for now, in french:
SOURCE_WEBSITES_FR = {

    'anobii': 'http://www.anobii.com',   # mostly popular in Italy (has 50M books catalogued)
    'decitre': 'http://www.decitre.fr',
    'amazon': 'http://www.amazon.fr',  # amazon purchased goodreads, but reviews not planned to be integrated..
    'fnac': 'http://www.fnac.com',
    'shelfari': 'http://www.shelfari.com',  # also an amazon co.. will be merged with gr!!

    # these sites are quite outdated and/or limited in term of reviews
    'critique-livre': 'http://www.critique-livre.fr',  # only 750 reviews on novel (roman)
    'senscritique': 'http://www.senscritique.com',  # only a number of top-list are available
    'guidelecture': 'http://www.guidelecture.com'  # outdated
}


SOURCE_WEBSITES = {
    'goodreads': 'http://www.goodreads.com',  # 10M reviews of 700k works
    'librarything': 'http://www.librarything.com',  # 2.5M reviews of 1M works
    'critiqueslibres': 'http://www.critiqueslibres.com',  # 100K critics  out of 45K livres, (in french only)
    'babelio': 'http://www.babelio.com' # ?? apparently 500 critiques/jour  (150K/an) !
}


REVIEW_PREFIX = "ReviewOf"
REVIEW_EXT = ".csv"

SCRAPED_OUTPUT_DIR = "/Users/mart/Temp/reviews"
# archive NOT under SCRAPED_OUTPUT_DIR as we recursively load review files from there
SCRAPED_ARCHIVE_DIR = "/Users/mart/Temp/reviews_archive"
REF_DATA_DIR = "/Users/mart/Temp/ref_data"
REF_ARCHIVE_DIR = "/Users/mart/Temp/ref_data/archive"


