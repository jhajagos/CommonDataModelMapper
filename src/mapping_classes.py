"""
Mapper classes which a define a method map which has an argument input_dict and returns a dict
"""

import json
import csv
import logging
from timeit import default_timer as timer
import os
import sqlalchemy as sa
import sys


class InputClass(object):
    """Superclass representing the abstract input source"""
    def fields(self):
        return []

    def required_fields(self):
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
        self.force_ascii = True
        self.input_class = input_class_obj

        if len(self.input_class.fields()):
            self.input_class_has_fields = True
        else:
            self.input_class_has_fields = False

        if self.force_ascii and sys.version_info[0] == 2:
            f = open(csv_file_name, 'rb')
        else:

            f = open(csv_file_name, newline='', encoding="utf-8")
        self.csv_dict = csv.DictReader(f)

        self.i = 1

    def __next__(self):
        row_dict = self.csv_dict.__next__()
        row_dict[":row_id"] = self.i

        if self.input_class_has_fields:
            fields = self.input_class.fields()
            for field in fields:
                if field not in row_dict:
                    row_dict[field] = ""

        self.i += 1
        return row_dict

    def next(self):

        if sys.version_info[0] == 2:
            row_dict = self.csv_dict.next()
        else:
            row_dict = self.csv_dict.__next__()

        row_dict[":row_id"] = self.i

        if self.input_class_has_fields:
            fields = self.input_class.fields()
            for field in fields:
                if field not in row_dict:
                    row_dict[field] = ""

        self.i += 1
        return row_dict

    def __iter__(self):
        return self


class OutputClassRealization(object):
    """Super Class for an output source"""
    pass


class OutputClassCSVRealization(OutputClassRealization):
    """Write output to CSV file"""
    def __init__(self, csv_file_name, output_class_obj, field_list=None, force_ascii=True):

        self.force_ascii = force_ascii

        if self.force_ascii and sys.version_info[0] == 2:
            self.fw = open(csv_file_name, "wb")
        else:
            self.fw = open(csv_file_name, "w", newline="", encoding="utf-8")

        self.output_class = output_class_obj
        if field_list is None:
            self.field_list = output_class_obj.fields()
        else:
            self.field_list = field_list

        self.force_ascii = force_ascii

        self.csv_writer = csv.writer(self.fw)
        self.csv_writer.writerow(self.field_list)

        self.i = 1

    def write(self, row_dict):
        row_to_write = []
        for field in self.field_list:
            if field in row_dict:
                value_to_write = row_dict[field]
                if self.force_ascii and sys.version_info[0] == 2:
                    if value_to_write.__class__ in (u"".__class__, "".__class__):
                        value_to_write = value_to_write.decode("ascii", "ignore").encode("ascii")
                row_to_write += [value_to_write]
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
    """Maps values to a code with a look-up dictionary"""

    def __init__(self, mapper_dict, field_name=None, key_to_map_to=None):
        self.mapper_dict = mapper_dict
        self.field_name = field_name
        self.key_to_map_to = key_to_map_to

    def map(self, input_dict):

        if self.field_name is None:
            if len(input_dict):
                key = list(input_dict.keys())[0]
            else:
                return {}

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
        with open(json_file_name) as f:
            self.mapper_dict = json.load(f)

    def map(self, input_dict):

        if len(input_dict):
            if self.field_name is None:
                key = list(input_dict.keys())[0]
            else:
                key = self.field_name

            if key in input_dict:
                value = input_dict[key]
            else:
                return {}

            if value in self.mapper_dict:
                mapped_dict_instance = self.mapper_dict[value]
                if mapped_dict_instance.__class__ == [].__class__:
                    mapped_dict_instance = mapped_dict_instance[0]
                    logging.error("Map '%s' to non-unique value" % value)

                return mapped_dict_instance
            else:
                return {}
        else:
            return {}


class CodeMapperClassSqliteJSONClass(CodeMapperClass):
    """For large JSON files we build a SQLite database and cache in memory what we access"""

    def __init__(self, json_file_name, field_name=None):
        self.field_name = field_name

        self.db_file_name = json_file_name + ".db3"
        self.json_file_name = json_file_name

        if os.path.exists(self.db_file_name):
            self.connection, self.meta_data = self._create_connection()
        else:
            self.connection, self.meta_data = self._build_sqlite_db()

        self.mapper_dict_cache = {}
        self.missed_mapper_dict_cache = {} # hold values that are missed so we don't make multiple expensive lookups to file

    def _create_connection(self):
        connection_string = "sqlite:///" + self.db_file_name
        engine = sa.create_engine(connection_string)
        connection = engine.connect()
        meta_data = sa.MetaData(connection, reflect=True)
        return connection, meta_data

    def _build_sqlite_db(self):

        if os.path.exists(self.db_file_name):
            os.remove(self.db_file_name)

        connection,  meta_data = self._create_connection()

        lookup_table = sa.Table("lookup_table", meta_data,
                                sa.Column("key_string", sa.String(255), index=True, unique=True),
                                sa.Column("json_value_text", sa.Text))

        meta_data.create_all()

        transaction = connection.begin()
        logging.info("Building SQLite database for '%s'" % self.json_file_name)
        try:
            with open(self.json_file_name, "r") as f:
                json_dict = json.load(f)

                for key in json_dict:

                    key_value = json_dict[key]
                    key_dict = {"key_string": key, "json_value_text": json.dumps(key_value)}
                    connection.execute(lookup_table.insert(key_dict))
        except:
            transaction.commit()
            raise

        transaction.commit()

        return connection, meta_data

    def _look_up_value(self, key):

        lookup_table = self.meta_data.tables["lookup_table"]
        sql_expression = lookup_table.select().where(lookup_table.c.key_string == key)
        cursor = self.connection.execute(sql_expression)
        rows = list(cursor)
        if len(rows):
            key_value = json.loads(rows[0].json_value_text)
            return key_value
        else:
            return None

    def map(self, input_dict):

        if len(input_dict):
            if self.field_name is None:
                key = list(input_dict.keys())[0]
            else:
                key = self.field_name

            if key in input_dict:
                value = input_dict[key]
            else:
                return {}

            if len(value):  # We only look for values that exist
                if value in self.mapper_dict_cache:  # We have seen this value before
                    mapped_dict_instance = self.mapper_dict_cache[value]
                    if mapped_dict_instance.__class__ == [].__class__:
                        mapped_dict_instance = mapped_dict_instance[0]
                        logging.error("Map '%s' to non-unique value selecting the first item" % value)

                    return mapped_dict_instance

                else: # The value is not in our cache

                    if value in self.missed_mapper_dict_cache:  # We check to see if the value is in our miss cache
                        self.missed_mapper_dict_cache[value] += 1
                        return {}
                    else:
                        read_from_db_value = self._look_up_value(value)
                        if read_from_db_value is None:
                            self.missed_mapper_dict_cache[value] = 1  # If the value is missed add to cache
                            return {}
                        else:
                            self.mapper_dict_cache[value] = read_from_db_value
                            mapped_dict_instance = read_from_db_value
                            if mapped_dict_instance.__class__ == [].__class__:
                                mapped_dict_instance = mapped_dict_instance[0]
                                logging.error("Map '%s' to non-unique value selecting the first item" % value)
                            return mapped_dict_instance
            else:
                return {}
        else:
            return {}


class IdentityMapper(MapperClass):
    """Simple maps to the same value"""

    def __init__(self):
        pass

    def map(self, input_dict):
        return input_dict


class HasNonEmptyValue(MapperClass):
    """Tests whether a field has a value other than '' or null"""

    def map(self, input_dict):
        key = list(input_dict.keys())[0]
        key_value = input_dict[key]

        if key_value is not None:
            if len(key_value):
                if key_value != "null":
                    return {"non_empty_value": True}

        return {}


class TransformMapper(MapperClass):
    """Applies a transformation for example, lower or upper case"""

    def __init__(self, func):
        self.func = func

    def map(self, input_dict):

        mapped_dict = {}
        for key in input_dict:
            mapped_dict[key] = self.func(input_dict[key])

        return mapped_dict


class FunctionMapper(MapperClass):
    def __init__(self, mapper_func, key_name="mapped_value"):
        self.mapper_func = mapper_func
        self.key_name = key_name

    def map(self, input_dict):
        return {self.key_name: self.mapper_func(input_dict)}


class PassThroughFunctionMapper(FunctionMapper):

    def map(self, input_dict):
        return self.mapper_func(input_dict)


class CaseMapper(MapperClass):
    """Case function returns an integer 0....n where it then evaluates"""
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


class CascadeKeyMapper(MapperClass):
    """Runs through mappers until one returns a results with a specific key"""

    def __init__(self, key, *mapper_classes):
        self.mapper_classes = mapper_classes
        self.key = key

    def map(self, input_dict):

        for mapper_class in self.mapper_classes:
            result_dict = mapper_class.map(input_dict)
            if self.key in result_dict:
                return result_dict

        return {}


class FilterHasKeyValueMapper(MapperClass):

    """Return only a single key which matches a key and has a value"""

    def __init__(self, keys_to_track, empty_values=("", "null"), empty_value=None):
        self.keys_to_track = keys_to_track
        self.empty_values = empty_values
        self.empty_value = empty_value

    def map(self, input_dict):

        for key in self.keys_to_track:
            if key in input_dict:
                if self.empty_value is None:
                    if input_dict[key] not in self.empty_values:
                        return {key: input_dict[key]}
                else:
                    if input_dict[key] != self.empty_value:
                        return {key: input_dict[key]}

        return {}


class SingleMatchAddValueMapper(MapperClass):
    """Match a key in the first position and then value and then eject a value"""

    def __init__(self, pattern_match, key_replace):
        key_to_match, value_to_match = pattern_match
        self.key_to_match = key_to_match
        self.value_to_match = value_to_match
        self.key_replace = key_replace

    def map(self, input_dict):
        if self.key_to_match in input_dict:
            value_to_test = input_dict[self.key_to_match]
            if value_to_test == self.value_to_match:
                new_key, new_value = self.key_replace
                input_dict[new_key] = new_value
                return input_dict
            else:
                return input_dict
        else:
            return input_dict


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
    """Translate keys in a dict to a different key"""

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

    def map(self, dict_to_map):
        return self.translate(dict_to_map)


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


class LeftStringMapper(MapperClass):
    """Cuts strings over a certain length, similar to the left function"""

    def __init__(self, maximum_string_length=1023):
        self.maximum_string_length = maximum_string_length

    def map(self, dict_to_map):

        new_dict = {}
        for key in dict_to_map:
            key_value = dict_to_map[key]
            if key_value.__class__ == "".__class__:
                new_dict[key] = key_value[0:self.maximum_string_length]
            else:
                new_dict[key] = key_value

        return new_dict


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

        #
        # logging.debug(input_dict)
        # logging.debug(mapped_dict)

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
    """Defines method for registering an imput and output connection"""

    def register(self, input_class_obj, output_class_obj, mapper_class_obj):
        self.directory_dict[(input_class_obj.__class__, output_class_obj.__class__)] = mapper_class_obj


class OutputClassDirectory(DirectoryClass):
    def register(self, output_class_obj, output_class_realization_obj):
        self.directory_dict[output_class_obj.__class__] = output_class_realization_obj


class RunMapper(object):
    """Executes the map"""
    pass


class RunMapperAgainstSingleInputRealization(RunMapper):
    """Main class for running a mapping process"""

    def __init__(self, input_class_realization_obj, input_output_directory_obj, output_directory_obj, output_class_func,
                 pre_map_func=None, post_map_func=None):
        self.input_class_realization_obj = input_class_realization_obj
        self.input_output_directory_obj = input_output_directory_obj
        self.output_directory_obj = output_directory_obj
        self.output_class_func = output_class_func

        self.pre_map_func = pre_map_func
        self.post_map_func = post_map_func

        self.rows_run = 0

        self.output_classes_written = []

    def run(self, n_rows=10000):

        i = 0
        j = 0

        mapping_results = {}  # Stores counts of how many rows are mapped to specific classes
        input_class = self.input_class_realization_obj.input_class.__class__

        global_start_time = timer()
        start_time = timer()
        logging.info("Mapping input %s" % input_class)
        for row_dict in self.input_class_realization_obj:

            if self.pre_map_func is not None:
                row_dict = self.pre_map_func(row_dict)

            output_class_obj = self.output_class_func(row_dict)
            output_class = output_class_obj.__class__

            if output_class in mapping_results:
                mapping_results[output_class] += 1
            else:
                mapping_results[output_class] = 1

            if output_class == NoOutputClass().__class__:
                pass  # logger("Row not mapped" + str(row_dict))
            else:
                output_class_instance = self.output_directory_obj[output_class]

                if output_class_instance not in self.output_classes_written:
                    self.output_classes_written += [output_class_instance]

                mapper_obj = self.input_output_directory_obj[(input_class, output_class)]

                try:
                    mapped_row_dict = mapper_obj.map(row_dict)
                except:
                    print(row_dict)
                    raise

                if self.post_map_func is not None:
                    mapped_row_dict = self.post_map_func(mapped_row_dict)

                output_class_instance.write(mapped_row_dict)

                j += 1
                #TODO: will need to add a call back function

            if i % n_rows == 0 and i > 0:
                end_time = timer()
                logging.info("Read %s rows and mapped %s rows in %s seconds" % (i, j - 1, end_time - start_time))
                start_time = end_time

            i += 1

        self.rows_run = i

        global_end_time = timer()
        total_time = global_end_time - global_start_time

        logging.info("Total time %s seconds" % total_time)
        if i:
            logging.info("Rate per %s rows: %s" % (n_rows, n_rows * (total_time * 1.0)/i,))
        else:
            logging.info("No rows")

        logging.info("%s" % mapping_results)

        for output_class_inst in self.output_classes_written:
            output_class_inst.close()
