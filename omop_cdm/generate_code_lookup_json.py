"""
    The goal is to build dictionaries that can be used to search for codes in
    sources and map to elements in the CDM vocabulary
"""

import csv
import json
import os
import pprint

def main(source_vocabulary_directory, output_json_directory=None, delimiter="\t"):

    if output_json_directory is None:
        output_json_directory = source_vocabulary_directory

    concept_csv = os.path.join(source_vocabulary_directory, "CONCEPT.csv")
    vocabularies = []

    with open(concept_csv, "rb") as f:
        dict_reader = csv.DictReader(f, delimiter=delimiter)
        for row_dict in dict_reader:
            vocabulary_id = row_dict["VOCABULARY_ID"]
            if vocabulary_id not in vocabularies:
                vocabularies += [vocabulary_id]

    for vocabulary in vocabularies:
        fields_to_key_on = ["CONCEPT_CODE", "CONCEPT_NAME"]

        vocabulary_name = "_".join(vocabulary.split(" "))
        for field_to_key_on in fields_to_key_on:
            file_vocabulary_name = field_to_key_on + "_" + vocabulary_name + ".json"
            path_vocabulary_name = os.path.join(output_json_directory, file_vocabulary_name)
            if not os.path.exists(path_vocabulary_name):
                print("Generating %s" % file_vocabulary_name)
                csv_file_name_to_keyed_json(concept_csv, path_vocabulary_name, field_to_key_on, ("VOCABULARY_ID", vocabulary))

    concept_relationship_csv = os.path.join(source_vocabulary_directory, "CONCEPT_RELATIONSHIP.CSV")
    concept_relationship_json = os.path.join(output_json_directory, "concept_relationship.json")
    csv_file_name_to_keyed_json(concept_relationship_csv, concept_relationship_json, "CONCEPT_ID_1", ("RELATIONSHIP_ID", "Maps to"))


def csv_file_name_to_keyed_json(csv_file_name, json_file_name, field_to_key_on, filter_pair=None, delimiter="\t"):
    """Create a keyed JSON file"""
    with open(csv_file_name, "rb") as f:
        dict_reader = csv.DictReader(f, delimiter=delimiter)
        result_dict = {}

        include_row = True
        if filter_pair is not None:
            field, filter_value = filter_pair
            include_row = False

        for row_dict in dict_reader:
            if filter_value is not None:
                field_value = row_dict[field]
                if field_value == filter_value:
                    include_row = True
                else:
                    include_row = False

            if include_row:
                key = row_dict[field_to_key_on]
                if key in result_dict:
                    if result_dict[key].__class__ == [].__class__:
                        result_dict[key] += [row_dict]
                    else:
                        result_dict[key] = [result_dict[key], row_dict]
                else:
                    result_dict[key] = row_dict

    with open(json_file_name, "w") as fw:
        json.dump(result_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":
    main("E:\\data\\vocab_download_v5_{680A7A12-692C-D9A0-351D-DB05A1E8A46D}")