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
    'critiqueslibres': 'http://www.critiqueslibres.com',  # 15K livres
    'decitre':'http://www.decitre.fr',
    'amazon': 'http://www.amazon.fr',
    'fnac': 'http://www.fnac.com',
    'senscritique': 'http://www.senscritique.com',
    'babelio': 'http://www.babelio.com',
    'critique-livre': 'http://www.critique-livre.fr',
    # these sites are quite outdated and/or just too few reviews to be considered
    'guidelecture': 'http://www.guidelecture.com'
}


SOURCE_WEBSITES_EN = {
    'goodreads': 'http://www.goodreads.com',
    'librarything': 'http://www.librarything.com'
}






# resource

SCRAPED_OUTPUT_DIR = "/Users/mouellet/Temp/"
SCRAPED_ARCHIVE_DIR = "~/Temp/archive"

# business rules




# trigger settings
    




# etc..
