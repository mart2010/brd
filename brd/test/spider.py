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




class TestCritiqueslibres(unittest.TestCase):

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_scrape_review_stat_is_ok(self):
        self.assertTrue(True)





class TestOtherSiteSpider(unittest.TestCase):
    pass








if __name__ == '__main__':
    unittest.main()




