# -*- coding: utf-8 -*-
import brd
from brd.scrapy.scrapy_utils import resolve_value
import json
import scrapy
from brd.scrapy.items import ReviewItem
import brd.scrapy.scrapy_utils as scrapy_utils
from datetime import datetime


__author__ = 'mouellet'


class BaseReviewSpider(scrapy.Spider):
    """
    Superclass of all reviews spider subclass
    """
    # to convert review Date string (to be defined by subclass)
    raw_date_format = None

    min_harvest_date = datetime(1900, 1, 1).date()

    def __init__(self, **kwargs):
        """
        Arguments passed to all Spider subclass harvesting review

        Spiders do NOT manage any load logic, they're only concerned
        with harvesting review (delegating parsing/outputing to pipelines)
        1. dump_filepath:
        2. works_to_harvest: list of dict with  work-ids (and additional info):
        [{'work_uid': x,
        'site_work_uid': yy (the other site work id to map lt's work_uid with)
        'last_harvest_dts': y,
        'nb_in_db': {'ENG': 12, 'FRE': 2, ..},
        'isbns': [x,y,..]}, {..}...]
        n.b. certain keywords are spider/context specific (ex. isbns...)
        """
        super(BaseReviewSpider, self).__init__(**kwargs)
        self.dump_filepath = kwargs['dump_filepath']
        # descending order is the default
        self.works_to_harvest = kwargs.get('works_to_harvest', {})

    def build_review_item(self, **kwargs):
        item = ReviewItem(site_logical_name=self.name, **kwargs)
        return item

    def get_dump_filepath(self):
        return self.dump_filepath

    def parse_review_date(self, raw_date):
        parse_date = None
        if raw_date:
            parse_date = datetime.strptime(raw_date, self.raw_date_format).date()
        return parse_date

    def parse_rating(self, rating):
        raise NotImplementedError


class LibraryThingWorkReview(BaseReviewSpider):
    """
    Spider only fetches #of reviews needed (by comparing #of review on Site vs DB).
    Reviews are then requested in desc order (latest-first) so only the first needed ones
    are emitted.
    TODO: to manage any delete reviews, should request more reviews (buffer) and use last_harvest_date
     for filter out

    """
    name = 'librarything'
    allowed_domains = ['www.librarything.com']

    ###########################
    # Control setting
    ###########################
    url_workreview = 'https://www.librarything.com/work/%s/reviews'
    url_formRequest = 'https://www.librarything.com/ajax_profilereviews.php'

    form_static_data = {'offset': '0', 'type': '3', 'container': 'wp_reviews'}
    # 'offset': 25 (i.e. skip first 25.. showing 26 to 50)
    # to set dynamically: 'showCount':25, 'languagePick':'fre', 'workid': '2371329', 'sort': '0'  (0=desc, 3=asc)
    # 'showCount': 25 (i.e. show 25 reviews, show all is set to 10'000 in lt)
    # other formData not mandatory:  bookid: , optionalTitle:, uniqueID: , mode: profile
    ##########################

    ###########################
    # Parse setting
    ###########################
    langs_root = '//div[@class="languagepick"]//text()'
    xpath_reviews = '//div[@class="bookReview"]'
    xpath_rtext_rating = './div[@class="commentText"]'
    xpath_user_date = './div[@class="commentFooter"]/span[@class="controlItems"]'
    raw_date_format = '%b %d, %Y'
    ###########################

    @property
    def start_requests(self):
        for i in xrange(len(self.works_to_harvest)):
            wid = self.works_to_harvest[i]['work_uid']
            req = scrapy.Request(self.url_workreview % wid, callback=self.parse_nbreview)
            req.meta['work-index'] = i
            req.meta['wid'] = wid
            yield req

    def parse_nbreview(self, response):

        def prepare_form(workid, langpick, n_in_page, n_in_db):
            """
            Using descending sort and showCount = nb-missing from DB
            guarantees incremental/initial harvesting to work properly
            """
            form_data = self.form_static_data
            form_data['workid'] = workid
            form_data['languagePick'] = langpick
            form_data['sort'] = '0'  # descending
            # TODO: if site allows deletion of review (nb_missing too low and incre would miss some...)
            nb_missing = n_in_page - n_in_db
            form_data['showCount'] = str(nb_missing)
            return form_data

        wid = response.url[response.url.index('/work/') + 6: response.url.index('/reviews')]
        requested_wid = response.meta['wid']
        dup_id = None
        # when work has duplicate id, must be able to link it to "master" (eg. work=17829 and work=13001031)
        if wid != requested_wid:
            dup_id = requested_wid

        work_index = response.meta['work-index']
        db_info = self.works_to_harvest[work_index]
        nb_db_dic = db_info.get('nb_in_db', {})
        nb_page_site = self.scrape_langs_nb(response)
        # now only need this for data integrity checks
        last_harvest_date_db = db_info.get('last_harvest_dts', None)

        for lang in nb_page_site:
            marc_code = brd.get_marc_code(lang, capital=False)
            nb_in_db = nb_db_dic.get(marc_code, 0)
            nb_in_site = nb_page_site[lang]
            if last_harvest_date_db is None or nb_in_site > nb_in_db:
                r = scrapy.FormRequest(self.url_formRequest,
                                       formdata=prepare_form(wid, marc_code, nb_in_site, nb_in_db),
                                       callback=self.parse_reviews)

                item = self.build_review_item(work_uid=wid, dup_uid=dup_id, review_lang=marc_code)
                r.meta['passed_item'] = item
                yield r

    def parse_reviews(self, response):
        for review_sel in response.xpath(self.xpath_reviews):
            sel1 = review_sel.xpath(self.xpath_rtext_rating)
            # TODO: issues when text review includes markup inline (text under markup is skipped)
            rtext = sel1.xpath('./text()').extract()[0]
            r_list = sel1.xpath('./span[@class="rating"]/img/@src').extract()  # gives list of [http://pics..../ss6.gif]
            if r_list or len(r_list) == 1:
                r = r_list[0]
                rating = r[r.rindex('pics/') + 5:]  # gives ss10.gif
            else:
                rating = None

            sel2 = review_sel.xpath(self.xpath_user_date)
            username = sel2.xpath('./a[starts-with(@href,"/profile/")]/@href').extract()[0]  # gives /profile/yermat
            rdate = sel2.xpath('./text()').extract()[0]  # gives :   |  Nov 22, 2012  |
            rawdate = rdate[rdate.index('|') + 1:rdate.rindex('|')].strip()

            item = response.meta['passed_item']
            item['username'] = username[username.rindex('/') + 1:]
            # for lt, username and userid are the same
            item['user_uid'] = item['username']
            item['rating'] = rating
            item['review'] = rtext
            item['review_date'] = rawdate
            item['parsed_review_date'] = self.parse_review_date(rawdate)
            item['parsed_rating'] = self.parse_rating(rating)
            yield item

    def scrape_langs_nb(self, response):
        """Extract language/nb of reviews (assuming English only when lang bar menu not found)
        (TODO: fix-this assumption later... some book only have reviews in one foreign lang)
        :return {'English':  34, 'French': 12, .. }
        """
        lang_codes_nb = {}
        list_l_n = response.xpath(self.langs_root).extract()
        if len(list_l_n) == 0:
            # 'Showing 4 of 4'  (when there are reviews) or nothing
            show_sel = response.xpath('//div[@id="mainreviews_reviewnav"]/text()')
            if show_sel:
                show_txt = show_sel.extract()[0]
                nb = int(show_txt[show_txt.rindex('of') + 2:])
            else:
                nb = 0
            lang_codes_nb['English'] = nb
        else:
            for i in xrange(len(list_l_n)):
                if list_l_n[i].find('(') != -1:
                    nb = list_l_n[i]
                    lang_codes_nb[list_l_n[i - 1]] = int(nb[nb.index('(') + 1:nb.index(')')])

        if len(list_l_n) == 1:
            # to log
            print("The page '%s' has language bar but only one lang" % response.ur)

        if u'All languages' in lang_codes_nb:
            lang_codes_nb.pop(u'All languages')
        return lang_codes_nb

    def parse_rating(self, rating):
        parsed_rating = None
        if rating:
            parsed_rating = int(rating[rating.index('ss') + 2:rating.index('.gif')])
        return parsed_rating


class GoodreadsReview(BaseReviewSpider):
    """
    This requires isbns in self.works_to_harvest for never harvested work.
    and relies on last_harvest_dts to filter needed reviews (no need for nb_in_db)
    """
    name = 'goodreads'
    allowed_domains = ['www.goodreads.com']
    ##################reload#########
    # Control setting
    ###########################  1582099855
    url_search = 'https://www.goodreads.com/search?utf8=%E2%9C%93&query='
    url_review = 'https://www.goodreads.com/book/show/%s?page=%d&sort=newest'
    # to avoid searching all isbns
    max_nb_search = 10

    ###########################
    # Parse setting
    ###########################
    no_results = '//h3[@class="searchSubNavContainer"]//text()'
    reviews_sel = '//div[starts-with(@id,"review_")]'
    raw_date_format = '%b %d, %Y'

    @property
    def start_requests(self):
        for i in xrange(len(self.works_to_harvest)):
            isbns = self.works_to_harvest[i].get('isbns')
            # search by isbn is required first to map gr work-uid
            if isbns:
                yield scrapy.Request(self.url_search + str(isbns[0]),
                                     meta={'work_index': i, 'nb_try': 0},
                                     callback=self.parse_search_resp)
            else:
                gr_work_id = self.works_to_harvest[i]['site_work_uid']
                assert(gr_work_id, 'Getting latest reviews requires gr work id')
                yield scrapy.Request(self.url_review % (gr_work_id, 1),
                                     meta={'work_index': i},
                                     callback=self.parse_reviews)

    def parse_search_resp(self, response):
        widx = response.meta['work_index']
        isbns = self.works_to_harvest[widx]['isbns']
        nb_try = response.meta['nb_try']
        nb_try += 1
        no_res = response.xpath(self.no_results).extract()
        # not found
        if 'Looking for a book?' in response.body or \
                (len(no_res) > 0 and no_res[0].startswith('No result')):
            if nb_try < len(isbns):
                yield scrapy.Request(self.url_search + str(isbns[nb_try]),
                                     meta={'work_index': widx, 'nb_try': nb_try},
                                     callback=self.parse_search_resp)
            else:
                print("No work found for '%s' with isbns: %s" % (str(self.works_to_harvest[widx]['work_uid']), str(isbns)))
                yield self.build_review_item(work_uid=self.works_to_harvest[widx]['work_uid'])
        # found it, map gr's id (cannot start harvesting, as reviews are ordered arbitrarily)
        else:
            url = response.url
            gr_work_id = url[url.index('/book/show/') + 11:]
            self.works_to_harvest[widx]['site_work_uid'] = gr_work_id
            self.works_to_harvest[widx]['last_harvest_dts'] = self.min_harvest_dae
            yield scrapy.Request(self.url_review % (gr_work_id, 1),
                                 meta={'work_index': widx},
                                 callback=self.parse_reviews)

    def parse_reviews(self, response):
        meta = response.meta
        widx = meta['work_index']
        last_page = meta.get('last_page')
        if last_page is None:
            # get how many pages of reviews (TODO:  max is 100...: see how to get missing ones)
            # last page is the text just before the "next page" link
            pageno_before_next = response.xpath('//a[@class="next_page"]/preceding-sibling::*[1]/text()')
            if len(pageno_before_next) > 0:
                last_page = int(pageno_before_next.extract()[0])
            else:
                last_page = 1

        url = response.url
        current_page = int(url[url.index('?page=') + 6:url.index('&sort=')])

        if current_page <= last_page:
            found_older = False
            authors_raw = response.xpath('//a[@class="authorName"]/child::*/text()')
            item = self.build_review_item(work_uid=self.works_to_harvest[widx]['work_uid'],
                                          site_work_uid=self.works_to_harvest[widx]['site_work_uid'],
                                          book_author=",".join(authors_raw.extract()),
                                          book_title=response.xpath('//h1[@class="bookTitle"]/text()').extract()[0].strip())

            reviews_sel = response.xpath(self.reviews_sel)
            # no review yet
            if len(reviews_sel) == 0:
                yield item
            else:
                for rev in reviews_sel:
                    self.process_onereview(item, widx, rev)
                    if item['parsed_review_date'] > self.works_to_havest[widx]['last_harvest_dts']:
                        yield item
                    else:
                        found_older = True
                        break
                if current_page != last_page and not found_older:
                    gr_work_id = self.works_to_harvest[widx]['site_work_uid']
                    yield scrapy.Request(self.url_review % (gr_work_id, current_page + 1),
                                         meta={'work_index': widx, 'last_page': last_page},
                                         callback=self.parse_reviews)

    def process_onereview(self, item, widx, rev):
        """
        Process one review in rev selector
        :return: Item
        """
        item['review_date'] = rev.xpath('.//a[@itemprop="publishDate"]/text()').extract()[0]  # u'Feb 14, 2016'
        item['parsed_review_date'] = self.parse_review_date(item['review_date'])
        # for gr, 0 star means No rating (however some user consider it as 0 rating!)
        nb_star = len(rev.xpath('.//span[@class="staticStar p10"]'))
        item['rating'] = str(nb_star) if nb_star > 0 else None
        item['parsed_rating'] = self.parse_rating(item['rating'])
        item['username'] = rev.xpath('.//a[@class="user"]/@title').extract()[0]  # u'Jon Liu'
        u_link = rev.xpath('.//a[@class="user"]/@href').extract()[0]  # u'/user/show/52104079-jon-liu'
        item['user_uid'] = u_link[u_link.index('/show/') + 6:]
        item['review'] = rev.xpath('.//span[starts-with(@id,"freeTextContainer")]/text()').extract()[0]
        item['likes'] = rev.xpath('.//span[@class="likesCount"]/text()').extract()[0]
        return item

    def parse_rating(self, rating):
        """
        Normalize gr's rating (which are 5-based star) on a 10-based star
        """
        parsed_rating = None
        if rating:
            parsed_rating = 2 * int(rating)
        return parsed_rating


class CritiquesLibresReview(BaseReviewSpider):
    """
    First Step: Fetch # of Reviews in Json format using request:
        # http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=0&limit=300
        (adjust: start=? and limit=?)

        Json :  {"total":"44489", "data":[ {"id":"", "titre":"", "nbrcrit":"", ...}, {"id":....}] }
        where total is the total number of books reviewed.

    Second step:  Fetch new reviews ready for loading.
    Note:
    1) takes only 4 min to harvest all reviews (period 2000-2014), so can be relauched
    in a single session without messing around with incremental harvesting
    """
    name = 'critiqueslibres'
    allowed_domains = ['www.critiqueslibres.com']  # used as 'hostname' in item field
    lang = 'FR'

    ###########################
    # Control setting
    ###########################
    items_per_page = 2000
    # set to -1, for proper initialization (for test purposes, set to any small values)
    max_nb_items = 10000  # -1
    ##########################

    ###########################
    # Parse_nb_reviews setting
    ###########################
    url_nb_reviews = "http://www.critiqueslibres.com/a.php?action=book&what=list&page=1&start=%d&limit=%d"
    xpath_allreviews = '//table[@width="100%" and @border="0" and @cellpadding="3"]/tr'  # for Review page (is there more stable xpath?)
    ##########################

    ###########################
    # Parse_review setting
    ###########################
    url_review = "http://www.critiqueslibres.com/i.php/vcrit/%d?alt=print"
    xpath_title = '//td[@class="texte"]/p/strong/text()'
    xpath_rating = './td[@class="texte"]/img[contains(@name,"etoiles")]/@name'
    xpath_date = './td[@class="texte"]/p[2]/text()'  # return:  " - BOURGES - 50 ans - 1 décembre 2004"
    xpath_username = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/text()'
    xpath_reviewer_uid = './td[@class="texte"]/p[2]/a[starts-with(@href,"/i.php/vuser")]/@href'  # return: "/i.php/vuser/?uid=32wdqee2334"
    ###########################


    def init_max_nb_items(self, response):
        res = json.loads(response.body[1:-1])
        self.max_nb_items = int(res['total'])

    def start_requests(self):
        if self.max_nb_items == -1:
            yield scrapy.Request(self.url_nb_reviews % (0, 2), callback=self.init_max_nb_items)

        for i in xrange(0, self.max_nb_items, self.items_per_page):
            yield scrapy.Request(self.url_nb_reviews % (i, self.items_per_page), callback=self.parse_nb_reviews)

    def parse_nb_reviews(self, response):
        pageres = json.loads(response.body[1:-1])

        if pageres['success'] != 0:
            raise IOError("Page result error for request: " + response.request)

        for review in pageres['data']:
            # Only reviews-eclaire considered for scraping (i.e. = #reviews - 1 )
            nreviews_eclair = int(review['nbrcrit']) - 1
            bookuid = review['id']
            # only crawls work with reviews not yet persisted
            if nreviews_eclair > 1:  # TODO: implement same logic as other with works_to_harvest instead of self.lookup_stored_nb_reviews(bookuid):
                lname, fname = self.parse_author(review['auteurstring'])
                item = ReviewItem(hostname=self.allowed_domains[0],
                                  site_logical_name=self.name,
                                  book_uid=bookuid,
                                  book_title=review['titre'],
                                  book_lang=self.lang,
                                  author_fname=fname,
                                  author_lname=lname)
                # trigger the 2nd Request
                request = scrapy.Request(self.url_review % int(bookuid), callback=self.parse_review)
                request.meta['item'] = item
                yield request

    def parse_review(self, response):
        passed_item = response.meta['item']

        if resolve_value(response.selector, self.xpath_title) != passed_item['book_title']:
            raise ValueError("Book title in webpage ('%s') different from Json ('%s')"
                             % (
                             resolve_value(response.selector, self.xpath_title), response.meta['item']['book_title']))

        allreviews = response.xpath(self.xpath_allreviews)
        rowno = 1
        for review_sel in allreviews:
            # iterate through 3 rows for each critics : row1: title_star, row2: username + date, row3: horizontal line
            if rowno == 1:
                passed_item['rating'] = resolve_value(review_sel, self.xpath_rating)
                # TODO: add the review which is now the review content as text
                rowno = 2
            elif rowno == 2:
                ruid = resolve_value(review_sel, self.xpath_reviewer_uid)
                rdate = resolve_value(review_sel, self.xpath_date)
                passed_item['username'] = resolve_value(review_sel, self.xpath_username),
                passed_item['user_uid'] = ruid[ruid.rfind("=") + 1:],
                passed_item['review_date'] = rdate[rdate.rfind("-") + 2:]
                rowno = 3
            else:
                rowno = 1
                yield passed_item

    def parse_review_date(self, review_date_str):
        month_name = review_date_str[(review_date_str.find(' ') + 1): review_date_str.rfind(' ')]
        month_no = scrapy_utils.mois[month_name]
        review_date_str = review_date_str.replace(month_name, month_no)
        return datetime.strptime(review_date_str, '%d %m %Y').date()

    def parse_author(self, author_str):
        i = author_str.index(', ')
        lname = author_str[0:i]
        fname = author_str[i + 2:]
        return (lname, fname)


class BabelioSpider(BaseReviewSpider):
    """
    Babelio has no global list to easily crawl for total #ofReviews.  Best approach is to
    use ISBN from persisted reviews and search reviews based on these.
    As a consequence, only reviews from already persisted book are scraped from this site.
    """
    name = 'babelio'
    allowed_domains = ['www.babelio.com']

    # Book_uid is defined in this site as 'title/id' (ex. 'Green-Nos-etoiles-contraires/436732'
    # tri=dt order by date descending
    review_url_param = "http://www.babelio.com/livres/%s/critiques?pageN=2&tri=dt"

    def __init__(self, **kwargs):
        super(BabelioSpider, self).__init__(BabelioSpider.name, **kwargs)
        pass

    def start_requests(self):
        pass
        # yield scrapy.Request(self.url_nb_reviews % (0, 2), callback=self.init_max_nb_items)


class DecitreSpider(BaseReviewSpider):
    """
    This is similar to Babelio, only a search can be easily implemented
    """
    pass


