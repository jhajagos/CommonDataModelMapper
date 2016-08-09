import unittest

import sys
import os

from mapping_classes import *


class Object1(InputClass):
    def fields(self):
        return ["id", "object_name", "object_code"]


class Object1Output(OutputClass):
    def fields(self):
        return ["id", "object_name", "object_code"]

class Object1OutputCaps(OutputClass):
    def fields(self):
        return ["ID", "OBJECT_NAME", "OBJECT_CODE"]


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

        in_obj_1 = InputClassCSVRealization("./test/input_object1.csv", Object1())

        in_dict = list(in_obj_1)
        self.assertEquals({":row_id": 1, "id": '234', "object_name": "ab", "object_code": '102'}, in_dict[0])


class TestOutputSourceRealizations(unittest.TestCase):

    def setUp(self):
        if os.path.exists("./test/write_csv_test.csv"):
            os.remove("./test/write_csv_test.csv")

    def test_write_csv(self):

        o_obj = OutputClassCSVRealization("./test/write_csv_test.csv", Object1Output())
        o_obj.write({"id": '234', "object_name": "ab", "object_code": '102'})


class TestBuildInputOutMapper(unittest.TestCase):
    def setUp(self):
        pass

    def test_identity_mapper(self):
        rules = ["id", "object_name", "object_code"]
        output_obj = build_input_output_mapper(rules)

        self.assertEquals(3, len(output_obj))


def test_output_func(void_dict):
    return Object1Output()

def test_output_caps_func(void_dict):
    return Object1OutputCaps()


class TestRunMapper(unittest.TestCase):

    def setUp(self):
        if os.path.exists("./test/output_obj1.csv"):
            os.remove("./test/output_obj1.csv")


    def test_run_identity_mapper(self):
        rules = ["id", "object_name", "object_code"]
        mapper_rules = build_input_output_mapper(rules)
        mapper_rules_class = InputOutputMapper(mapper_rules)

        in_out_map_obj = InputOutputMapperDirectory()
        in_out_map_obj.register(Object1(), Object1Output(), mapper_rules_class)

        in_obj_1 = InputClassCSVRealization("./test/input_object1.csv", Object1())

        output_directory_obj = OutputClassDirectory()
        output_realization = OutputClassCSVRealization("./test/output_obj1.csv", Object1Output())

        output_directory_obj.register(Object1Output(),output_realization)

        map_runner_obj = RunMapperAgainstSingleInputRealization(in_obj_1, in_out_map_obj, output_directory_obj, test_output_func)
        map_runner_obj.run()

        output_realization.close()

        with open("./test/input_object1.csv", "rb") as f:
            t1 = f.read()

        with open("./test/output_obj1.csv", "rb") as f:
            t2 = f.read()

        self.assertEquals(t1, t2)

    def test_identity_mapper_with_translate(self):
        rules = [("id", "ID"), ("object_name", "OBJECT_NAME"), ("object_code", "OBJECT_CODE")]
        mapper_rules = build_input_output_mapper(rules)
        mapper_rules_class = InputOutputMapper(mapper_rules)

        in_out_map_obj = InputOutputMapperDirectory()
        in_out_map_obj.register(Object1(), Object1OutputCaps(), mapper_rules_class)

        in_obj_1 = InputClassCSVRealization("./test/input_object1.csv", Object1())

        output_directory_obj = OutputClassDirectory()
        output_realization = OutputClassCSVRealization("./test/output_obj1_caps.csv", Object1OutputCaps())

        output_directory_obj.register(Object1OutputCaps(), output_realization)

        map_runner_obj = RunMapperAgainstSingleInputRealization(in_obj_1, in_out_map_obj, output_directory_obj,
                                                                test_output_caps_func)
        map_runner_obj.run()

        output_realization.close()




if __name__ == '__main__':
    unittest.main()
