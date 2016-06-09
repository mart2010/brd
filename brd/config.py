# -*- coding: utf-8 -*-
__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The BRD Project"


DATABASE = {
    'host': 'localhost',
    # 'host': '192.168.0.28',
    'database': 'brd',
    'port': '54355',
    'user': 'brd',
    'password': 'brd'
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
    'goodreads': 'http://www.goodreads.com',  # updated: 50M reviews from 800K (not updated) works
    'librarything': 'http://www.librarything.com',  # 2.7M reviews of 1M works
    'critiqueslibres': 'http://www.critiqueslibres.com',  # 100K critiques sur 45K livres (french only)
    'babelio': 'http://www.babelio.com' # ?apparently 500 critiques/jour  (150K/an) !
}


SCRAPED_OUTPUT_DIR = "/Users/mart/Temp/reviews"
# archive NOT under SCRAPED_OUTPUT_DIR as we recursively load review files from there
SCRAPED_ARCHIVE_DIR = "/Users/mart/Temp/reviews_archive"
REF_DATA_DIR = "/Users/mart/Temp/ref_data"
REF_ARCHIVE_DIR = "/Users/mart/Temp/ref_data/archive"



# detectlang works best with prior (especially on small text)
# prior prob are derived from a sample of reviews with longer text
prior_prob_map = \
    {   'en': 0.94054839038317164193,
     'fr': 0.02304769603390122412,
     'es': 0.00898924577295931053,
     'it': 0.00486969277622062963,
     'ar': 0.00395536516707438209,
     'id': 0.00287404504234620922,
     'pt': 0.00253899877867298636,
     'de': 0.00241089285432734232,
     'nl': 0.00205886687050416428,
     'fa': 0.00119237052660176371,
     'tr': 0.00109162924053350281,
     'cs': 0.00074362077978151952,
     'bg': 0.00059133391468660894,
     'ru': 0.00058201022906855319,
     'fi': 0.00055835990457397276,
     'sv': 0.00044950777004122433,
     'el': 0.00041319845775627552,
     'ro': 0.00033095293827993009,
     'da': 0.00031025890434717221,
     'hr': 0.00030790903236213377,
     'pl': 0.00028084760337314269,
     'sk': 0.00020701614164967684,
     'no': 0.00019670702584434691,
     'hu': 0.00019011222382181967,
     'et': 0.00017464855011382477,
     'ca': 0.00016289919018863256,
     'vi': 0.00015971549266051597,
     'th': 0.00012446741288493935,
     'lv': 0.00012264815715458701,
     'uk': 0.000085201810038167985223,
     'lt': 0.000070268752584859183542,
     'he': 0.000062915927341351804035,
     'sl': 0.000053364834757002012097,
     'af': 0.000039796219101457466407,
     'bn': 0.000035399684419772641547,
     'so': 0.000034944870487184556217,
     'ja': 0.000025014766292344693170,
     'ko': 0.000024256743071364550953,
     'tl': 0.000017510336404641285219,
     'mk': 0.000013417011011348517246,
     'sq': 0.000009172080973859720829,
     'cy': 0.000006443197378331208847,
     'ta': 0.000002274069662940426652,
     'sw': 0.000001970860374548369765,
     'ur': 0.000001667651086156312878,
     'ml': 0.000001440244119862270213,
     'hi': 0.000000909627865176170661,
     'ne': 0.000000303209288392056887,
     'te': 0.000000227406966294042665,
     'mr': 0.000000227406966294042665,
     'gu': 0.000000075802322098014222,
     'kn': 0.000000075802322098014222
    }

# sql used to generate above map:
# select concat('''',code2, '''', ': ', ratio, ', ') from
# (select code2, count(*) / sum(count(*)) OVER() as ratio
# from review r join language l on r.review_lang = l.code
# where char_length(review) >= 100
# group by 1) as foo
# order by foo.ratio desc