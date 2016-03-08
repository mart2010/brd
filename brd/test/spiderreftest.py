# -*- coding: utf-8 -*-

import unittest
from mockresponse import fake_response_from_file
import brd.scrapy.spiders.reference as spiderref


class TestLtReference(unittest.TestCase):

    def test_parse_ref(self):
        spider = spiderref.WorkReference(wids=None)
        meta = {'wid': '1'}
        # validate parse_work()
        ref_items = spider.parse_work(
                fake_response_from_file("mockobject/Workdetails_Rien_de_grave_LibraryThing.html",
                                        response_type="Html",
                                        url="https://www.librarything.com/work/1/workdetails",
                                        meta=meta))

        item = ref_items.next()
        self.assertEqual(item['work_refid'], '1')
        self.assertIsNone(item.get('dup_refid'))
        self.assertEqual(item['title'], u'Rien de grave')
        self.assertEqual(item['authors'], u'Justine Lévy')
        self.assertEqual(item['authors_code'], u'levyjustine')
        self.assertEqual(item['original_lang'], u'French')
        self.assertEqual(item['ori_lang_code'], u'FRE')
        self.assertEqual(item['mds_code'], u'848')
        self.assertEqual(item['mds_text'], u'Literature-->French-->Authors, French and French miscellany')
        self.assertIsNone(item.get('lc_subjects'))
        self.assertEqual(item['popularity'], 97109)
        self.assertEqual(item['other_lang_title'],
                         u'French : Rien de grave|--|'
                         u'German : Nicht so tragisch|--|'
                         u'Italian : Niente di grave|--|'
                         u'Swedish : Inget allvarligt')
        try:
            ref_items.next()
        except StopIteration, e:
            self.assertTrue(e is not None)

    def test_parse_2authors_ref(self):
        spider = spiderref.WorkReference(wids=None)
        meta = {'wid': '1'}
        # validate parse_work()
        ref_items = spider.parse_work(
                fake_response_from_file("mockobject/Workdetails_2authors_work.html",
                                        response_type="Html",
                                        url="https://www.librarything.com/work/1/workdetails",
                                        meta=meta))

        item = ref_items.next()
        self.assertEqual(item['title'], u"Nick & Norah's Infinite Playlist")
        self.assertEqual(item['authors'], u'Rachel Cohn;David Levithan')
        self.assertEqual(item['authors_code'], u'cohnrachel;levithandavid')
        self.assertEqual(item['original_lang'], u'English')
        self.assertEqual(item['ori_lang_code'], u'ENG')
        self.assertEqual(item['mds_code'], u'813.6')
        self.assertEqual(item['mds_text'], u'Literature-->American And Canadian-->Fiction-->21st Century')
        self.assertEqual(item['lc_subjects'], u'Love->Fiction|--|New York (N.Y.)->Fiction|--|Rock groups->Fiction')
        self.assertEqual(item['popularity'], 2552)
        self.assertEqual(item['other_lang_title'],
                         u'Czech : Nick a Norah: až do ochraptění|--|'
                         u'Dutch : Nick en Norah|--|'
                         u'French : Une nuit a New York|--|'
                         u'German : Nick & Norah - Soundtrack einer Nacht|--|'
                         u'Swedish : Nick & Norahs oändliga låtlista')


if __name__ == '__main__':
    unittest.main()




