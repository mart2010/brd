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
    'critiqueslibres': 'http://www.critiqueslibres.com',  # 15K livres, 50K critics
    'decitre':'http://www.decitre.fr',
    'amazon': 'http://www.amazon.fr',
    'fnac': 'http://www.fnac.com',
    'babelio': 'http://www.babelio.com',
    # these sites are quite outdated and/or limited in term of reviews
    'critique-livre': 'http://www.critique-livre.fr',  # only 750 reviews on novel (roman)
    'senscritique': 'http://www.senscritique.com',  # only a number of top-list are available
    'guidelecture': 'http://www.guidelecture.com'  # outdated
}


SOURCE_WEBSITES_EN = {
    'goodreads': 'http://www.goodreads.com',
    'librarything': 'http://www.librarything.com'
}




# resource

SCRAPED_OUTPUT_DIR = "/Users/mouellet/Temp/"
SCRAPED_ARCHIVE_DIR = "/Users/mouellet/Temp/archive"

# business rules

# Minimum nb of reviews before loading book from website
MIN_NB_REVIEWS = 5





# trigger settings
    




# etc..
