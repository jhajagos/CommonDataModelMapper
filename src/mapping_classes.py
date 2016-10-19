

"""
Mapping class
"""

import json
import csv
import logging
from timeit import default_timer as timer

class InputClass(object):
    """Superclass representing the abstract input source"""
    def fields(self):
        return []


class OutputClass(object):
    """Superclass representing the abstract source that the input source will be transformed into"""
    def fields(self):
        return []


class NoOutputClass(OutputClass):
    """Class representing when a row cannot be transformed"""
    def fields(self):
        return None


class InputClassRealization(object):
    """Superclass representing the source that will be read from. Subclasses should be iterable and return
        a dict of fields with values
    """
    pass


class InputClassCSVRealization(InputClassRealization):
    """Class for representing a CSV source to be read from"""
    def __init__(self, csv_file_name, input_class_obj):
        self.csv_file_name = csv_file_name

        self.input_class = input_class_obj

        f = open(csv_file_name, "rb")
        self.csv_dict = csv.DictReader(f)

        self.i = 1

    def next(self):
        row_dict = self.csv_dict.next()
        row_dict[":row_id"] = self.i
        self.i += 1
        return row_dict

    def __iter__(self):
        return self


class OutputClassRealization(object):
    """Super Class for an output source"""
    pass


class OutputClassCSVRealization(OutputClassRealization):
    """Write output to CSV file"""
    def __init__(self, csv_file_name, output_class_obj, field_list=None):
        self.fw = open(csv_file_name, "wb")
        self.output_class = output_class_obj
        if field_list is None:
            self.field_list = output_class_obj.fields()
        else:
            self.field_list = field_list

        self.csv_writer = csv.writer(self.fw)
        self.csv_writer.writerow(self.field_list)

        self.i = 1

    def write(self, row_dict):
        row_to_write = []
        for field in self.field_list:
            if field in row_dict:
                row_to_write += [row_dict[field]]
            else:
                row_to_write += [""]
        self.csv_writer.writerow(row_to_write)
        self.i += 1

    def close(self):
        self.fw.close()


class MapperClass(object):
    """A superclass that maps a {'key1': 'value1'} -> {'f1': f1(key1), 'f2': f2(key1)}"""


class CodeMapperClass(MapperClass):
    """Maps a code to a single or multiple codes in output"""
    pass


class CoderMapperStaticClass(CodeMapperClass):
    """A static defined mapper"""
    pass


class CodeMapperDictClass(CodeMapperClass):

    def __init__ (self, mapper_dict, field_name=None, key_to_map_to=None):
        self.mapper_dict = mapper_dict
        self.key_to_map_to = key_to_map_to
        self.field_name = field_name

    def map(self, input_dict):

        if self.field_name is None:
            key = input_dict.keys()[0]
        else:
            key = self.field_name

        if self.key_to_map_to is None:
            key_to_map_to = "mapped_value"
        else:
            key_to_map_to = self.key_to_map_to

        if key in input_dict:
            value = input_dict[key]
        else:
            return {}

        if value in self.mapper_dict:
            return {key_to_map_to: self.mapper_dict[value]}
        else:
            return {}


class CoderMapperJSONClass(CodeMapperClass):
    """A code mapper that reads code from a JSON dict of dicts"""

    def __init__(self, json_file_name, field_name=None):
        self.field_name = field_name
        with open(json_file_name, "r") as f:
            self.mapper_dict = json.load(f)

    def map(self, input_dict):

        if self.field_name is None:
            key = input_dict.keys()[0]
        else:
            key = self.field_name

        if key in input_dict:
            value = input_dict[key]
        else:
            return {}

        if value in self.mapper_dict:
            return self.mapper_dict[value]
        else:
            return {}


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


class CaseMapper(MapperClass):
    """Case function returns an integer 0....n where it then evalut"""
    def __init__(self, case_function, *map_cases):
        self.case_function = case_function
        self.map_cases = map_cases

    def map(self, input_dict):
        case_value = self.case_function(input_dict)
        return self.map_cases[case_value].map(input_dict)


class ChainMapper(MapperClass):
    """Chain together separate mappers and applies each mapper from left to right."""
    def __init__(self, *mapper_classes):
        self.mapper_classes = mapper_classes

    def map(self, input_dict):
        result_dict = {}
        i = 0
        for mapper_class in self.mapper_classes:
            if i == 0:
                result_dict = mapper_class.map(input_dict)
            else:
                result_dict = mapper_class.map(result_dict)

            i += 1

        return result_dict


class CascadeMapper(MapperClass):
    """Runs through mappers until one returns a results"""

    def __init__(self, *mapper_classes):
        self.mapper_classes = mapper_classes

    def map(self, input_dict):

        for mapper_class in self.mapper_classes:
            result_dict = mapper_class.map(input_dict)

            if len(result_dict):
                return result_dict

        return {}


class FilterHasKeyValueMapper(MapperClass):

    def __init__(self, keys_to_track, empty_value=""):
        self.keys_to_track = keys_to_track
        self.empty_value = empty_value

    def map(self, input_dict):

        for key in self.keys_to_track:
            if key in input_dict:
                if input_dict[key] != self.empty_value:
                    return {key: input_dict[key]}

        return {}



class ReplacementMapper(MapperClass):
    """Translate a string by exact match"""

    def __init__(self, mapping_dict):
        self.mapping_dict = mapping_dict

    def map(self, dict_to_translate):
        translated_dict = {}
        for key in dict_to_translate:
            value_str = dict_to_translate[key]
            if value_str in self.mapping_dict:
                replacement_str = self.mapping_dict[value_str]
                translated_dict[key] = replacement_str
            else:
                translated_dict[key] = value_str

        return translated_dict


class ConstantMapper(MapperClass):
    """Always returns the same thing in a map operation no matter what the input is"""

    def __init__(self, mapping_result):
        self.mapping_result = mapping_result

    def map(self, void_dict):
        return self.mapping_result


class KeyTranslator(object):
    """Translate keys in a dict"""

    def __init__(self, translate_dict):
        self.translate_dict = translate_dict

    def translate(self, dict_to_map):
        translated_dict = {}
        for key in dict_to_map:
            try:
                if key in self.translate_dict:
                    translated_dict[self.translate_dict[key]] = dict_to_map[key]
                else:
                    translated_dict[key] = None
            except TypeError:
                print(dict_to_map)
                raise

        return translated_dict


def single_key_translator(map_field_from, map_field_to):
    """Create a simple key translator mapping a single key to a second key"""
    return KeyTranslator({map_field_from: map_field_to})


class IdentityTranslator(KeyTranslator):
    def __init__(self):
        pass

    def translate(self, dict_to_map):
        return dict_to_map


class ConcatenateMapper(object):
    """Concatenate several fields together"""

    def __init__(self, delimiter, *fields):
        self.delimiter = delimiter
        self.fields = fields

    def map(self, dict_to_map):
        cat_string = ""
        for field in self.fields:
            if field in dict_to_map:
                cat_string += dict_to_map[field] + self.delimiter

        if len(cat_string):
            return {self.delimiter.join(self.fields): cat_string[:-1]}
        else:
            return {self.delimiter.join(self.fields): ""}


class InputOutputMapperInstance(object):
    """A single mapping rule"""
    def __init__(self, map_function=IdentityMapper(), key_translator=IdentityTranslator()):
        self.map_function = map_function
        self.key_translator = key_translator

    def map(self, input_dict):
        return self.key_translator.translate(self.map_function.map(input_dict))


class InputOutputMapper(object):
    """Basic class that applies a map and a key translation"""
    def __init__(self, field_mapper_instances):
        self.field_mapper_instances = field_mapper_instances

    def map(self, input_dict):
        mapped_dict = {}

        for field_mapper_instance in self.field_mapper_instances:
            field, mapper_instance = field_mapper_instance
            field_dict = {}
            if field.__class__ == tuple:
                pass
            else:
                field = (field, )

            for single_field in field:
                try:
                    single_field_value = input_dict[single_field]
                except KeyError:
                    logging.error("Cannot find key %s" % single_field)
                    logging.error(input_dict)
                    raise
                field_dict[single_field] = single_field_value

            mapped_dict_instance = mapper_instance.map(field_dict)
            for key in mapped_dict_instance:
                mapped_dict[key] = mapped_dict_instance[key]

        return mapped_dict

    def __len__(self):
        return len(self.field_mapper_instances)


def build_input_output_mapper(mapped_field_pairs):
    """Build an input output mapper based on the following patterns
        [e1, e2, ... , en] where e1 is of type
             1)  str -> Identity Map, Identity Field Translate
             2) (str1, str2) -> Identity Map, Translate(str1 -> str2)
             3) (str1, MapperClassInstance, str2)
             4) (str1, MapperClassInstance, Dict)
             5) (str1, MapperClassInstance, TranslatorClassInstance)
             6) ((str1, str2), MapperClassInstance)
             7) ((str1, str2), MapperClassInstance, Dict)
             8) ((str1, str2), MapperClassInstance, TranslatorClassInstance)
    """

    string_types = ("".__class__, u"".__class__)
    input_output_mapper_instance_list = []
    for mapped_field in mapped_field_pairs:

        if mapped_field.__class__ in ("".__class__, u"".__class__): # Case 1: Identity Field Map
            input_output_mapper_instance_list += [(mapped_field, InputOutputMapperInstance())]
        else:
            if len(mapped_field) == 2: # Case 2
                if mapped_field[0].__class__ in  string_types and mapped_field[1].__class__ in string_types:
                    input_output_mapper_instance_list += [(mapped_field[0],
                                                           InputOutputMapperInstance(key_translator=single_key_translator(*mapped_field)))]
                else:
                    input_output_mapper_instance_list += [(mapped_field[0], InputOutputMapperInstance(map_function=mapped_field[1]))]

            elif len(mapped_field) == 3:
                mapper_class_obj = mapped_field[1]
                if mapped_field[2].__class__ == dict:
                    key_translator_obj = KeyTranslator(mapped_field[2])
                elif mapped_field[2].__class__ in string_types:
                    key_translator_obj = single_key_translator(mapped_field[0], mapped_field[2])

                else:
                    key_translator_obj = mapped_field[2]

                input_output_mapper_instance_list += [(mapped_field[0], InputOutputMapperInstance(mapper_class_obj, key_translator_obj))]

    return InputOutputMapper(input_output_mapper_instance_list)


class DirectoryClass(object):
    """Provides lookup between input_class_name and output_class_name"""

    def __init__(self):
        self.directory_dict = {}

    def __getitem__(self, item):
        return self.directory_dict[item]


class InputOutputMapperDirectory(DirectoryClass):
    """"""
    def register(self, input_class_obj, output_class_obj, mapper_class_obj):
        self.directory_dict[(input_class_obj.__class__, output_class_obj.__class__)] = mapper_class_obj


class OutputClassDirectory(DirectoryClass):
    def register(self, output_class_obj, output_class_realization_obj):
        self.directory_dict[output_class_obj.__class__] = output_class_realization_obj


class RunMapper(object):
    """Executes the map"""
    pass


class RunMapperAgainstSingleInputRealization(RunMapper):
    def __init__(self, input_class_realization_obj, input_output_directory_obj, output_directory_obj, output_class_func):
        self.input_class_realization_obj = input_class_realization_obj
        self.input_output_directory_obj = input_output_directory_obj
        self.output_directory_obj = output_directory_obj
        self.output_class_func = output_class_func

    def run(self, n_rows=10000):

        i = 0
        j = 0

        mapping_results = {} # Stores counts of how many rows are mapped to specific classes
        input_class = self.input_class_realization_obj.input_class.__class__

        global_start_time = timer()
        start_time = timer()
        logging.info("Mapping input %s" % input_class)
        for row_dict in self.input_class_realization_obj:
            output_class_obj = self.output_class_func(row_dict)
            output_class = output_class_obj.__class__

            if output_class in mapping_results:
                mapping_results[output_class] += 1
            else:
                mapping_results[output_class] = 1

            if output_class == NoOutputClass().__class__:
                pass#logger("Row not mapped" + str(row_dict))
            else:
                output_class_instance = self.output_directory_obj[output_class]
                mapper_obj = self.input_output_directory_obj[(input_class, output_class)]
                mapped_row_dict = mapper_obj.map(row_dict)

                output_class_instance.write(mapped_row_dict)

                j += 1
                #TODO: will need to add a call back function

            if i % n_rows == 0 and i > 0:
                end_time = timer()
                logging.info("Read %s rows and mapped %s rows in %s seconds" % (i, j, end_time - start_time))
                start_time = end_time

            i += 1

        global_end_time = timer()
        total_time = global_end_time - global_start_time

        logging.info("Total time %s seconds" % total_time)
        logging.info("Rate per %s rows: %s" % (n_rows, n_rows * (total_time * 1.0)/i,))

        logging.info("%s" % mapping_results)