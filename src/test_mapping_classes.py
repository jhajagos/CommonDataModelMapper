import unittest

import sys
import os

from mapping_classes import *


class TestMappers(unittest.TestCase):

    def test_identity(self):
        im = IdentityMapper().map({"a": 1})
        self.assertEquals({"a": 1}, im)

    def test_translator(self):
        ucase_mapper = TransformMapper(lambda x : x.upper())
        tm = ucase_mapper.map({"a": "abcd", "b": "ABcd"})
        self.assertEquals({"a": "ABCD", "b": "ABCD"}, tm)


class TestTranslators(unittest.TestCase):

    def test_test_translator(self):

        kt_obj = KeyTranslator({"a": "f", "b": "x", "c": "z"})
        trans_dict = kt_obj.translate({"a": 1, "b": 2, "h": 3})

        self.assertEquals({"f": 1, "x": 2, "h": None}, trans_dict)

if __name__ == '__main__':
    unittest.main()
