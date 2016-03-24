# -*- coding: utf-8 -*-

from brd.scrapy.items import ReviewItem


import unittest
from mockresponse import fake_response_from_file
import brd.scrapy.spiders.reviews as spiderreviews
import brd.config as config
import datetime


class TestLtReview(unittest.TestCase):

    def mock_spider(self, wids, to_date=datetime.datetime.now()):
        spider = spiderreviews.LibraryThing(
            dump_filepath='dummy',
            to_date=to_date,
            works_to_harvest=[{'work_refid': wid} for wid in wids])
        return spider

    def test_start_request(self):
        wids = ['1111111', '2222222']
        spider = self.mock_spider(wids)
        req_gen = spider.start_requests()

        r = req_gen.next()
        self.assertEqual(r.url, spider.url_mainpage % wids[0])
        self.assertEqual(r.meta['work_index'], 0)
        r = req_gen.next()
        self.assertEqual(r.url, spider.url_mainpage % wids[1])
        try:
            req_gen.next()
            self.fail("no more request expected")
        except StopIteration:
            pass

    def test_mainpage_noreview(self):
        wids = ['3440235', 'dummy']
        spider = self.mock_spider(wids)
        meta = {'work_index': 0}
        item_gen = spider.parse_mainpage(fake_response_from_file("mockobject/LTWorkmain_%s_norev.html" % wids[0],
                                                                 url=spider.url_mainpage % "master",
                                                                 response_type="Html",
                                                                 meta=meta))
        item = item_gen.next()
        self.assertEqual(item['site_logical_name'], 'librarything')
        self.assertEqual(item['work_refid'], "master")
        self.assertEqual(item['dup_refid'], wids[0])
        self.assertEqual(item['tags_n'], u'1;1;1;1;1;1;1;1')
        self.assertEqual(item['tags_t'], u'21st century__&__biography__&__fiction__&__FRA 848 LAF 2001__&__French__&__Haiti__&__littérature québécoise__&__Quebec')
        try:
            item_gen.next()
            self.fail("no more item expected")
        except StopIteration:
            pass

    # although there is one review, we skip it as no information on how may is available
    def test_mainpage_onereview_nolang(self):
        wids = ['5266585']
        spider = self.mock_spider(wids)
        meta = {'work_index': 0}
        item_gen = spider.parse_mainpage(fake_response_from_file("mockobject/LTWorkmain_%s_onerev_nolang.html" % wids[0],
                                                                 url=spider.url_mainpage % "master",
                                                                 response_type="Html",
                                                                 meta=meta))
        item = item_gen.next()
        self.assertEqual(item['tags_n'], u'1;2;2;1;1;2;1;2;1;1;1;7;1;2;1;1;1;4;1;4;1;3;1;1;4;1;1;7;1;1')
        self.assertEqual(item['tags_t'], u'2008__&__21st century__&___global_reads__&__AF__&__BTBA longlist 2012__&__Canada__&__Canadian__&__Canadian literature__&__contemporary literature__&__diaspora__&__ethnic identity__&__fiction__&__French literature__&__Haiti__&__Haitian/Canadian/Japanese__&__home__&__importjan__&__Japan__&__Japan - Novel__&__littérature québécoise__&__my library__&__novel__&__postmodern__&__Q4 12__&__Quebec__&__stream of consciousness__&__to-buy__&__to-read__&__world fiction__&__writing')


    def test_mainpage_revs_nolang(self):
        wids = ['413508']
        spider = self.mock_spider(wids)
        meta = {'work_index': 0}

        req_gen = spider.parse_mainpage(fake_response_from_file("mockobject/LTWorkmain_%s_revs_nolang.html" % wids[0],
                                                                url=spider.url_mainpage % wids[0],
                                                                response_type="Html",
                                                                meta=meta))
        form_req = req_gen.next()
        self.assertEqual(spider.works_to_harvest[0]['last_harvest_date'], spider.min_harvest_date)

        self.assertEqual(form_req.meta['work_index'], 0)

        form_body = form_req.body
        self.assertTrue(form_body.find('workid=413508') != -1)
        self.assertTrue(form_body.find('sort=0') != -1)
        self.assertTrue(form_body.find('languagePick=all') != -1)
        self.assertTrue(form_body.find('showCount=9') != -1)

        passed_item = form_req.meta['passed_item']
        self.assertEqual(passed_item['review_lang'], u'und')
        self.assertEqual(passed_item['work_refid'], wids[0])
        self.assertIsNone(passed_item.get('dup_refid'))
        self.assertIsNone(passed_item.get('tags_t'))
        self.assertIsNone(passed_item.get('tags_n'))
        try:
            req_gen.next()
            self.fail("no more request expected")
        except StopIteration:
            pass

    def test_mainpage_revs_manylang(self):
        wids = ['2371329']
        spider = self.mock_spider(wids)
        meta = {'work_index': 0}

        req_gen = spider.parse_mainpage(fake_response_from_file("mockobject/LTWorkmain_%s_manylang.html" % wids[0],
                                                                url=spider.url_mainpage % wids[0],
                                                                response_type="Html",
                                                                meta=meta))
        nb_req = 0
        for form_req in req_gen:
            nb_req += 1
            form_body = form_req.body
            self.assertTrue(form_body.find('workid=2371329') != -1)
            passed_item = form_req.meta['passed_item']
            self.assertIsNone(passed_item.get('tags_t'))
            self.assertIsNone(passed_item.get('tags_n'))

            the_lang = passed_item.get('review_lang')
            self.assertTrue(the_lang in (u'eng', u'fre', u'ger', u'dut', u'spa', u'cat', u'fin', u'rus', u'ita'))
            if the_lang == u'eng':
                self.assertTrue(form_body.find('languagePick=eng') != -1)
                self.assertTrue(form_body.find('showCount=38') != -1)
            elif the_lang == u'fre':
                self.assertTrue(form_body.find('languagePick=fre') != -1)
                self.assertTrue(form_body.find('showCount=5') != -1)
            elif the_lang == u'ger':
                self.assertTrue(form_body.find('languagePick=ger') != -1)
                self.assertTrue(form_body.find('showCount=1') != -1)
            elif the_lang == u'dut':
                self.assertTrue(form_body.find('languagePick=dut') != -1)
                self.assertTrue(form_body.find('showCount=4') != -1)
            elif the_lang == u'spa':
                self.assertTrue(form_body.find('languagePick=spa') != -1)
                self.assertTrue(form_body.find('showCount=4') != -1)
            elif the_lang == u'cat':
                self.assertTrue(form_body.find('languagePick=cat') != -1)
                self.assertTrue(form_body.find('showCount=2') != -1)
            elif the_lang == u'fin':
                self.assertTrue(form_body.find('languagePick=fin') != -1)
                self.assertTrue(form_body.find('showCount=1') != -1)
            elif the_lang == u'rus':
                self.assertTrue(form_body.find('languagePick=rus') != -1)
                self.assertTrue(form_body.find('showCount=1') != -1)
            elif the_lang == u'ita':
                self.assertTrue(form_body.find('languagePick=ita') != -1)
                self.assertTrue(form_body.find('showCount=1') != -1)
        self.assertEqual(nb_req, 9)

    def test_parse_reviews(self):
        wid = ['2371329']
        spider = self.mock_spider(wid)
        # mock dependency
        spider.works_to_harvest[0]['last_harvest_date'] = spider.min_harvest_date
        # validate parse_reviews()
        meta = {'work_index': 0, 'passed_item': ReviewItem()}
        review_items = spider.parse_reviews(fake_response_from_file("mockobject/LTRespReviews_%s.html" % wid[0],
                                                                    response_type="Html",
                                                                    meta=meta))
        usernames = []
        userids = []
        parsed_dates = []
        parsed_ratings = []
        parsed_likes = []
        for item in review_items:
            usernames.append(item['username'])
            userids.append(item['user_uid'])
            parsed_dates.append(item['parsed_review_date'])
            if item.get('parsed_likes'):
                parsed_likes.append(item.get('parsed_likes'))
            if item.get('parsed_rating'):
                parsed_ratings.append(item.get('parsed_rating'))

            if item['username'] == u'briconella':
                self.assertIsNone(item.get('parsed_rating'))
                self.assertEqual(item['review_text'], u"Le meilleur d'Amélie Nothomb. J'ai découvert qu'il est au programme des terminales!")

            if item['username'] == u'yermat':
                self.assertEqual(1, item['parsed_likes'])
            else:
                self.assertIsNone(item.get('parsed_likes'))

        all_users = u"yermat==soniaandree==grimm==briconcella==Cecilturtle"
        self.assertEqual(all_users, u"==".join(usernames))
        self.assertEqual(all_users, u"==".join(userids))
        self.assertEqual([1], parsed_likes)
        self.assertEqual([10, 8, 6, 8], parsed_ratings)
        self.assertEqual([datetime.date(2012, 11, 22),
                          datetime.date(2009, 2, 25),
                          datetime.date(2007, 3, 14),
                          datetime.date(2007, 3, 4),
                          datetime.date(2006, 5, 22)], parsed_dates)

    def test_parse_reviews_nbwithin(self):
        wid = ['7571386']

        # mock a spider with specific last-harvest and launch dates
        spider = self.mock_spider(wid, to_date=datetime.date(2013, 8, 7))

        spider.works_to_harvest[0]['last_harvest_date'] = datetime.date(2013, 4, 4)
        meta = {'work_index': 0, 'passed_item': ReviewItem()}
        review_items = spider.parse_reviews(fake_response_from_file("mockobject/LTRespReviews_%s.html" % wid[0],
                                                                    response_type="Html",
                                                                    meta=meta))
        nb = 0
        # only expected to have the 3 duplicated reviews done on the 4/4/2013
        for rev in review_items:
            nb += 1
        self.assertEqual(3, nb)

        # otherwise initial load should have
        spider = self.mock_spider(wid)
        spider.works_to_harvest[0]['last_harvest_date'] = spider.min_harvest_date
        review_items = spider.parse_reviews(fake_response_from_file("mockobject/LTRespReviews_%s.html" % wid[0],
                                                                    response_type="Html",
                                                                    meta=meta))
        nb = 0
        for item in review_items:
            nb += 1
            self.assertTrue(item['username'])
            self.assertTrue(item['user_uid'])
            self.assertTrue(item['parsed_review_date'] < datetime.datetime.now().date())
            self.assertTrue(item['review'])
            if item.get('rating'):
                self.assertTrue(1 <= item['parsed_rating'] <= 10)
            if item.get('likes'):
                try:
                    n_likes = int(item['likes'])
                    self.assertEquals(n_likes, item['parsed_likes'])
                except ValueError:
                    pass

        self.assertEqual(127, nb)


if __name__ == '__main__':
    unittest.main()




