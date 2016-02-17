# -*- coding: utf-8 -*-

from brd.scrapy.items import ReviewItem


import unittest
from mockresponse import fake_response_from_file
import brd.scrapy.spiders.reviews as spiderreviews
import brd.config as config


class TestLtReview(unittest.TestCase):

    def mock_spider(self, wid):
        spider = spiderreviews.LibraryThingWorkReview(
            begin_period='1-1-2000',
            end_period='1-1-2016',
            dump_filepath='dummy',
            reviews_order='asc',
            works_to_harvest=[{'work-site-id': wid, 'last_harvest_dts': None, 'nb_in_db': {}}])
        return spider


    def test_start_request(self):
        wid = '2371329'
        spider = self.mock_spider(wid)
        req_gen = spider.start_requests

        r = req_gen.next()
        meta = r.meta
        self.assertEqual(r.url, spider.url_workreview % wid)
        self.assertEqual(meta['work-index'], 0)
        try:
            req_gen.next()
        except StopIteration:
            pass


    def test_parse_nbreview_withManylangs(self):
        wid = '2371329'
        spider = self.mock_spider(wid)
        # validate parse_nb_review()
        meta = {'work-index': 0}
        formreq_gen = spider.parse_nbreview(fake_response_from_file("mockobject/Reviews_lt_%s_manyLangs.html" % wid,
                                                                    url=spider.url_workreview % wid,
                                                                    response_type="Html",
                                                                    meta=meta))
        nreq = 0
        for f in formreq_gen:
            self.assertEqual(f.url, spider.url_formRequest)
            lib = f.body.index('languagePick=')+13
            lie = f.body.index('&',lib)
            lang = f.body[lib:lie]
            self.assertTrue(lang in ['eng', 'fre', 'spa', 'dut', 'cat', 'rus', 'ger', 'ita', 'fin'])
            print "for workid %s the formbody is %s" % (wid, f.body)
            nreq += 1
        self.assertEqual(9, nreq)

    def test_parse_reviews(self):
        wid = '2371329'
        spider = self.mock_spider(wid)
        # validate parse_reviews()
        meta = {'wid': wid}
        review_items = spider.parse_reviews(fake_response_from_file("mockobject/RespReview_lt_%s.html" % wid,
                                                                    response_type="Html",
                                                                    meta=meta))

        for item in review_items:
            # self.assertTrue(item[].find())
            self.assertEqual(item['work_uid'], wid)
            #print "the item" + str(item)


    def test_parse_nbreview_onlyOne(self):
        wid = '413508'
        spider = self.mock_spider(wid)
        meta = {'work-index': 0}
        req_gen = spider.parse_nbreview(fake_response_from_file("mockobject/Reviews_lt_%s_onlyEnglish.html" % wid,
                                                                url=spider.url_workreview % wid,
                                                                response_type="Html",
                                                                meta=meta))
        one_req = req_gen.next()
        try:
            req_gen.next()
        except StopIteration:
            pass

        self.assertEqual(one_req.url, spider.url_formRequest)
        lib = one_req.body.index('languagePick=')+13
        lie = one_req.body.index('&',lib)
        lang = one_req.body[lib:lie]
        self.assertEqual(lang, 'eng')



class TZZZestCritiqueslibres(unittest.TestCase):

    def setUp(self):
        self.spider = spiderreviews.CritiquesLibresReview(period='1-1-2001_31-12-2015')
        # overwrite rules for test purposes
        config.MIN_NB_REVIEWS = 2
        self.spider.stored_nb_reviews = {"200": "1", "400": "1", "500": "16"}

    def tearDown(self):
        pass

    def test_parse_nb_of_review_is_ok(self):

        review_url = "file://mockobject/Critiques-%d.html"
        self.spider.url_review = review_url

        # ---------- Trigger the first parse() with mock json ---------- #
        # for Spider running outside the engine (not using scrapy shell command)
        # it seems we have to callback explicitly with the Response
        request_asresp_generator = self.spider.parse_nb_reviews(fake_response_from_file("mockobject/review_list.json"))

        requests_actual = set()
        for r in request_asresp_generator:
            requests_actual.add(str(r))

        bookid_expected = (200, 300, 400)
        s = "<GET " + review_url + ">"
        requests_expected = set([s % i for i in bookid_expected])

        self.assertEqual(requests_expected, requests_actual)


    def test_parse_review_is_ok(self):

        bookid_expected = (200, 300, 400)
        # ---------- Trigger manually 2nd parse() with 1st response ---------- #
        item_param = ReviewItem(hostname=self.spider.allowed_domains[0])

        for ri in bookid_expected:
            # mock field set in parse_nb_items
            item_param['book_uid'] = ri
            item_param['book_title'] = {200: u"Le Chevalier Oublié", 300: u"Eden Hôtel, Tome 2 : Ernesto", 400: u"Le Chevalier Oublié2"}[ri]
            item_param['book_lang'] = 'FR'
            item_param['site_logical_name'] = 'thecritiqtues'
            item_param['hostname'] = 'thehost'

            meta  = {'item': item_param}
            items = self.spider.parse_review(fake_response_from_file("mockobject/Critiques-%d.html" % ri, response_type="Html", meta=meta))

            count = 0
            for item in items:
                # self.assertTrue( item[''] is not None)
                for i, v in item_param.items():
                    self.assertEqual(item[i], v)
                self.assertTrue(item.get('reviewer_pseudo', 'notset') != 'notset')
                self.assertTrue(item.get('review_rating', 'notset') != 'notset')
                self.assertTrue(item.get('review_date', 'notset') != 'notset')
                self.assertTrue(item.get('book_title', 'notset') != 'notset')
                self.assertTrue(item.get('reviewer_uid', 'notset') != 'notset')

                self.assertTrue(item.get('review_text', 'notset') == 'notset')
                self.assertTrue(item.get('book_isbn', 'notset') == 'notset')
                self.assertTrue(item.get('derived_title_sform', 'notset') == 'notset')
                self.assertTrue(item.get('derived_review_date', 'notset') == 'notset')
                count += 1

            if ri == "200":
                self.assertEquals(2, count)
            elif ri == "300":
                self.assertEquals(0, count)
            elif ri == "400":
                self.assertEquals(2, count)


if __name__ == '__main__':
    unittest.main()




