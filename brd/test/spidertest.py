from brd.scrapy.items import ReviewBaseItem

__author__ = 'mouellet'

import unittest


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

from mockresponse import fake_response_from_file
import brd.scrapy.spiders.reviewspiders as reviewspiders


class TestCritiqueslibres(unittest.TestCase):

    def setUp(self):
        self.clibresSpider = reviewspiders.CritiquesLibresSpider()
        # overwrite rules for test purposes
        self.clibresSpider.min_nb_reviews = 2
        self.clibresSpider.nbreviews_stored = {"200": "1", "400": "1", "500": "16"}


    def tearDown(self):
        pass


    def test_scrape_review_stat_is_ok(self):
        review_url = "file://mockobject/Critiques-%d.html"
        self.clibresSpider.review_url_param = review_url

        # ---------- Trigger the first parse() with mock json ---------- #
        # Spider running outside the engine (not using scrapy shell command)
        # and it seems we have to callback explicitly with the Response
        request_asresp = self.clibresSpider.parse(fake_response_from_file("mockobject/review_list.json"))

        requests_actual = set()
        for r in request_asresp:
            requests_actual.add(str(r))

        bookid_expected = (200, 300, 400)
        s = "<GET " + review_url + ">"
        requests_expected = set([s % i for i in bookid_expected])

        self.assertEqual(requests_expected, requests_actual)


        # ---------- Trigger manually 2nd parse() with 1st response ---------- #
        item_param = ReviewBaseItem()
        item_param['hostname'] = self.clibresSpider.allowed_domains[0]

        for ri in bookid_expected:
            item_param['book_uid'] = ri
            meta  = {'item': item_param}
            items = self.clibresSpider.parse_review(fake_response_from_file("mockobject/Critiques-%d.html" % ri,
                                                                            response_type="Html",
                                                                            meta=meta))
            countrev = 0
            for i in items: countrev += 1

            #TODO: adjust for begin/end-date once it is implemented
            if ri == "200":
                self.assertEquals(2, countrev)
            elif ri == "300":
                self.assertEquals(0, countrev)
            elif ri == "400":
                self.assertEquals(2, countrev)




if __name__ == '__main__':
    unittest.main()




