

"""
Mapping class
"""

import json
import csv


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


def single_key_translator(map_field_from, map_field_to):
    return KeyTranslator({map_field_from: map_field_to})


class IdentityTranslator(KeyTranslator):
    def __init__(self):
        pass
    def translate(self, dict_to_map):
        return dict_to_map


class RunMapper(object):
    """Executes the map"""
    pass


class InputOutputMapperInstance(object):
    def __init__(self, map_function=IdentityMapper(), key_translator=IdentityTranslator()):
        self.map_function = map_function
        self.key_translator = key_translator

    def map(self, input_dict):
        return self.key_translator.translate(self.map_function.map(input_dict))


class InputOutputMapper(object):
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
                single_field_value = input_dict[single_field]
                field_dict[single_field] = single_field_value

            mapped_dict_instance = mapper_instance.map(field_dict)
            for key in mapped_dict_instance:
                mapped_dict[key] = mapped_dict_instance[key]

        return mapped_dict


def build_input_output_mapper(mapped_field_pairs):
    """Build an input output mapper based on the following patterns
        [e1, e2, ... , en] where e1 is of type
             1)  str -> Identity Map, Identity Field Translate
             2) (str1, str2) -> Identity Map, Translate(str1 -> str2)
             3) (str1, MapperClassInstance, str2)
             4) (str1, MapperClassInstance, Dict)
             5) (str1, MapperClass, TranslatorClassInstance)
             6) ((str1, str2), MapperClassInstance)
             7) ((str1, str2), MapperClassInstance, Dict)
             8) ((str1, str2), MapperClassInstance, TranslatorClassInstance)
    """

    input_output_mapper_instance_list = []
    for mapped_field in mapped_field_pairs:

        if mapped_field.__class__ in ("".__class__, u"".__class__): # Case 1: Identity Field Map
            input_output_mapper_instance_list += [(mapped_field, InputOutputMapperInstance())]
        else:
            if mapped_field.__class__ == tuple:
                if len(mapped_field) == 2:
                    if mapped_field[0].__class__ in ("".__class__, u"".__class__) and \
                        mapped_field[1].__class__ in ("".__class__, u"".__class__):
                        input_output_mapper_instance_list += [(mapped_field[0],
                                                               InputOutputMapperInstance(key_translator=single_key_translator(*mapped_field)))]
            else:
                pass

    #    input_output_mapper_instance_list += [(mapped_field[0],
    #                                           InputOutputMapperInstance(key_translator=single_key_translator(*mapped_field)))]
    #
    # if field_func_translator_triplets is not None:
    #     for field_func_translator_triplet in field_func_translator_triplets:
    #         field, func, translator = field_func_translator_triplet
    #         input_output_mapper_instance_list += [field, InputOutputMapperInstance(func, translator)]

    return input_output_mapper_instance_list


class DirectoryClass(object):

    """Provides lookup between input_class_name and output_class_name"""

    def __init__(self):
        self.directory_dict = {}

    def __getitem__(self, item):
        return self.directory_dict[item]


class InputOutputMapperDirectory(DirectoryClass):
    def register(self, input_class_obj, output_class_obj, mapper_class_obj):
        self.directory_dict[(input_class_obj.__class__, output_class_obj.__class__)] = mapper_class_obj


class OutputClassDirectory(DirectoryClass):
    def register(self, output_class_obj, output_class_realization_obj):
        self.directory_dict[output_class_obj.__class__] = output_class_realization_obj


class RunMapperAgainstSingleInputRealization(object):
    def __init__(self, input_class_realization_obj, input_output_directory_obj, output_directory_obj, output_class_func):
        self.input_class_realization_obj = input_class_realization_obj
        self.input_output_directory_obj = input_output_directory_obj
        self.output_directory_obj = output_directory_obj
        self.output_class_func = output_class_func

    def run(self):

        input_class = self.input_class_realization_obj.input_class.__class__

        for row_dict in self.input_class_realization_obj:
            output_class_obj = self.output_class_func(row_dict)
            output_class = output_class_obj.__class__

            #TODO Add No NoOutputClass Logic

            output_class_instance = self.output_directory_obj[output_class]
            mapper_obj = self.input_output_directory_obj[(input_class, output_class)]
            mapped_row_dict = mapper_obj.map(row_dict)
            output_class_instance.write(mapped_row_dict)

            #TODO: will need to add a call back function