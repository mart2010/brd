# -*- coding: utf-8 -*-

from brd.scrapy.items import ReviewBaseItem

__author__ = 'mouellet'

import unittest
from mockresponse import fake_response_from_file
import brd.scrapy.spiders.reviewspiders as reviewspiders


# Most useful  assert functions:
#
# assert: base assert allowing you to write your own assertions
# assertEqual(a, b): check a and b are equal
# assertNotEqual(a, b): check a and b are not equal
# assertIn(a, b): check that a is in the item b
# assertNotIn(a, b): check that a is not in the item b
# assertFalse(a): check that the value of a is False
# assertTrue(a): check the value of a is True
# assertIsInstance(a, TYPE): check that a is of type "TYPE"
# assertRaises(ERROR, a, args): check that when a is called with args that it raises ERROR



class TestCritiqueslibres(unittest.TestCase):

    def setUp(self):
        self.spider = reviewspiders.CritiquesLibresSpider(begin_period='1/1/2001', end_period='31/12/2015')
        # overwrite rules for test purposes
        self.spider.min_nb_reviews = 2
        self.spider.stored_nb_reviews = {"200": "1", "400": "1", "500": "16"}


    def tearDown(self):
        pass


    def test_parse_nb_of_review_is_ok(self):

        review_url = "file://mockobject/Critiques-%d.html"
        self.spider.url_review = review_url

        # ---------- Trigger the first parse() with mock json ---------- #
        # Spider running outside the engine (not using scrapy shell command)
        # and it seems we have to callback explicitly with the Response
        request_asresp_generator = self.spider.parse(fake_response_from_file("mockobject/review_list.json"))

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
        item_param = ReviewBaseItem()
        item_param['hostname'] = self.spider.allowed_domains[0]

        for ri in bookid_expected:
            item_param['book_uid'] = ri
            item_param['book_title'] = {200: u"Le Chevalier Oublié", 300: u"Eden Hôtel, Tome 2 : Ernesto", 400: u"Le Chevalier Oublié2"}[ri]

            meta  = {'item': item_param}
            items = self.spider.parse_review(fake_response_from_file("mockobject/Critiques-%d.html" % ri, response_type="Html", meta=meta))
            countrev = sum(1 for i in items)

            if ri == "200":
                self.assertEquals(2, countrev)
            elif ri == "300":
                self.assertEquals(0, countrev)
            elif ri == "400":
                self.assertEquals(2, countrev)



class TestGuideLecture(unittest.TestCase):

    def setUp(self):
        self.spider = reviewspiders.GuidelectureSpider(begin_period='1/1/2001', end_period='31/12/2015')
        self.spider.min_nb_reviews = 2
        # string in DB to be stored in utf-8, so should mock object
        self.spider.stored_nb_reviews = {u"A la feuille de rose, maison turque": "1", u"A la vue à la mort": "2"}

    def tearDown(self):
        pass

    def test_parse_nb_of_review_is_ok(self):

        review_url = "file://mockobject/Guide-%s.html"
        # self.spider.review_url_param = review_url

        # ---------- Trigger the first parse() with local html ---------- #
        request_asresp_generator = self.spider.parse_nb_reviews(fake_response_from_file("mockobject/A_Z_Guide_lecture_pageA.html", response_type="Html"))

        for r in request_asresp_generator:
            print "request is " + str(r)









if __name__ == '__main__':
    unittest.main()




