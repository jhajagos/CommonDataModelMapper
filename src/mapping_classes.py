

"""
Mapping class
"""


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
    pass


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
    pass
# A mapper will need to be translated into another format


class RuntimeMapper(MapperClass):
    pass


class RuntimeDictMapper(MapperClass):
    pass


class IdentityMapper(MapperClass):
    """Simple maps to the same value"""

    def __init__(self):
        pass

    def map(self, dict):
        return dict


class TransformMapper(MapperClass):
    """Applies a translation, for example, lower or upper case"""

    def __init__(self, func):
        self.func = func

    def map(self, dict):
        mapped_dict = {}
        for key in dict:
            mapped_dict[key] = self.func(dict[key])

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