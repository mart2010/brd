# -*- coding: utf-8 -*-
__author__ = 'mart2010'
__copyright__ = "Copyright 2016, The BRD Project"

import unittest

import brd.scrapy.spiderref as spiderref
from mockresponse import mock_from_file


class TestLtReference(unittest.TestCase):

    def test_parse_ref(self):
        spider = spiderref.WorkReference(works_to_harvest=None, dump_filepath=None)
        meta = {'wid': '1'}
        # validate parse_work()
        ref_items = spider.parse_work(
                mock_from_file("mockobject/Workdetails_Rien_de_grave_LibraryThing.html",
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
        self.assertEqual(item['popularity'], u'97,109')
        self.assertEqual(item['other_lang_title'],
                         u'French : Rien de grave__&__'
                         u'German : Nicht so tragisch__&__'
                         u'Italian : Niente di grave__&__'
                         u'Swedish : Inget allvarligt')
        try:
            ref_items.next()
        except StopIteration, e:
            self.assertTrue(e is not None)

    def test_parse_2authors_ref(self):
        spider = spiderref.WorkReference(works_to_harvest=None, dump_filepath=None)
        meta = {'wid': '1'}
        # validate parse_work()
        ref_items = spider.parse_work(
                mock_from_file("mockobject/Workdetails_2authors_work.html",
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
        self.assertEqual(item['lc_subjects'], u'Love->Fiction__&__New York (N.Y.)->Fiction__&__Rock groups->Fiction')
        self.assertEqual(item['popularity'], u'2,552')
        self.assertEqual(item['other_lang_title'],
                         u'Czech : Nick a Norah: až do ochraptění__&__'
                         u'Dutch : Nick en Norah__&__'
                         u'French : Une nuit a New York__&__'
                         u'German : Nick & Norah - Soundtrack einer Nacht__&__'
                         u'Swedish : Nick & Norahs oändliga låtlista')


if __name__ == '__main__':
    unittest.main()




