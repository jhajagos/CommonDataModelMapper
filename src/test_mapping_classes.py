import unittest

import sys
import os

from mapping_classes import *


class Object1(InputClass):
    def fields(self):
        return ["id","object_name","object_code"]


class Object1Output(OutputClass):
    def fields(self):
        return ["id", "object_name", "object_code"]

class TestMappers(unittest.TestCase):

    def test_identity(self):
        im = IdentityMapper().map({"a": 1})
        self.assertEquals({"a": 1}, im)

    def test_translator(self):
        ucase_mapper = TransformMapper(lambda x : x.upper())
        tm = ucase_mapper.map({"a": "abcd", "b": "ABcd"})
        self.assertEquals({"a": "ABCD", "b": "ABCD"}, tm)


class TestTranslators(unittest.TestCase):

    def test_translator(self):

        kt_obj = KeyTranslator({"a": "f", "b": "x", "c": "z"})
        trans_dict = kt_obj.translate({"a": 1, "b": 2, "h": 3})

        self.assertEquals({"f": 1, "x": 2, "h": None}, trans_dict)

    def test_translator_json(self):

        cdx_obj = CoderMapperJSONClass("./test/code_mapper.json")
        mapped_code = cdx_obj.map({"code": "101"})

        self.assertEquals({"code_id": 702}, mapped_code)


class TestInputSourceRealizations(unittest.TestCase):

    def test_read_csv(self):

        in_obj = InputClassCSVRealization("./test/input_object1.csv", Object1())

        in_dict = list(in_obj)
        self.assertEquals({":row_id": 1, "id": '234', "object_name": "ab", "object_code": '102'}, in_dict[0])


class TestBuildInputOutMapper(unittest.TestCase):
    pass

if __name__ == '__main__':
    unittest.main()
