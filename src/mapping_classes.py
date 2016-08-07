

"""
Mapping class
"""

import json
import csv

class InputClass(object):
    """Superclass representing the abstract input source"""
    pass


class OutputClass(object):
    """Superclass representing the abstract source that the input source will be transformed into"""
    pass


class NoOutputClass(OutputClass):
    """Class representing when a row cannot be transformaed"""
    pass


class InputClassRealization(object):
    """Superclass representing the source that will be read from. Subclasses should be iterable and return
        a dict of fields with values
    """
    pass


class InputClassCSVRealization(InputClassRealization):
    """Class for representing a CSV source to be read from"""
    def __init__(self, csv_file_name, InputClassObj):
        self.csv_file_name = csv_file_name

        self.input_class_obj = InputClassObj

        f = open(csv_file_name, "rb")
        self.csv_dict = csv.DictReader(f)

    def next(self):
        return self.csv_dict.next()

    def __iter__(self):
        return self



class MapperClass(object):
    """A superclass that maps a {'key1': 'value1'} -> {'f1': f1(key1), 'f2': f2(key1)}"""


class CodeMapperClass(MapperClass):
    """Maps a code to a single or multiple codes in output"""
    pass


class CoderMapperStaticClass(CodeMapperClass):
    """A static defined mapper"""
    pass


class CoderMapperJSONClass(CodeMapperClass):
    """A code mapper that reads code from a JSON dict of dicts"""

    def __init__(self, json_file_name):
        with open(json_file_name, "r") as f:
            self.mapper_dict = json.load(f)

    def map(self, input_dict):
        key = input_dict.keys()[0]
        value = input_dict[key]

        if value in self.mapper_dict:
            return self.mapper_dict[value]
        else:
            return None


class RuntimeMapper(MapperClass):
    pass


class RuntimeDictMapper(MapperClass):
    pass


class IdentityMapper(MapperClass):
    """Simple maps to the same value"""

    def __init__(self):
        pass

    def map(self, input_dict):
        return input_dict


class TransformMapper(MapperClass):
    """Applies a translation, for example, lower or upper case"""

    def __init__(self, func):
        self.func = func

    def map(self, input_dict):
        mapped_dict = {}
        for key in input_dict:
            mapped_dict[key] = self.func(input_dict[key])

        return mapped_dict


class KeyTranslator(object):
    """Translate keys in a dict"""

    def __init__(self, translate_dict):
        self.translate_dict = translate_dict

    def translate(self, dict_to_map):
        translated_dict = {}
        for key in dict_to_map:
            if key in self.translate_dict:
                translated_dict[self.translate_dict[key]] = dict_to_map[key]
            else:
                translated_dict[key] = None

        return translated_dict


class RunMapper(object):
    """Executes the map"""
    pass