"""
    The goal is to build JSON dictionaries that can be used to search for codes in
    sources and map to elements in the CDM vocabulary.
"""

import csv
import json
import os
import argparse
import sys


def open_csv_file(file_name, mode="r"):

    ver_info = sys.version_info[0]
    if ver_info == 2:
        return open(file_name, mode=mode + "b")
    else:
        return open(file_name, newline="", mode=mode)


def main(source_vocabulary_directory, output_json_directory=None, delimiter="\t"):
    """Build files for needed vocabulary"""
    if output_json_directory is None:
        output_json_directory = source_vocabulary_directory

    concept_csv = os.path.join(source_vocabulary_directory, "CONCEPT.csv")
    vocabularies = []

    # Determine which vocabularies are in the concept file
    print("Scanning '%s'" % os.path.abspath(concept_csv))
    with open_csv_file(concept_csv, "r") as f:
        dict_reader = csv.DictReader(f, delimiter=delimiter)
        i = 0
        for row_dict in dict_reader:
            vocabulary_id = row_dict["VOCABULARY_ID".lower()]
            if vocabulary_id not in vocabularies:
                vocabularies += [vocabulary_id]
            i += 1

    print("Read %s lines" % i)
    print("Found %s vocabularies" % len(vocabularies))

    # Generate first pass of converting concepts into a JSON lookup file
    # Generate one for concept_name and one for concept_code
    for vocabulary in vocabularies:
        fields_to_key_on = ["CONCEPT_CODE".lower(), "CONCEPT_NAME".lower()]

        if vocabulary is not None:
            vocabulary_name = "_".join(vocabulary.split(" "))
            for field_to_key_on in fields_to_key_on:
                file_vocabulary_name = field_to_key_on + "_" + vocabulary_name + ".json"
                path_vocabulary_name = os.path.join(output_json_directory, file_vocabulary_name)
                if not os.path.exists(path_vocabulary_name):
                    print("Generating '%s'" % file_vocabulary_name)
                    csv_file_name_to_keyed_json(concept_csv, path_vocabulary_name, field_to_key_on,
                                                [("VOCABULARY_ID".lower(), vocabulary), ("INVALID_REASON".lower(), "")])

    concept_relationship_csv = os.path.join(source_vocabulary_directory, "CONCEPT_RELATIONSHIP.csv")
    concept_relationship_json = os.path.join(output_json_directory, "concept_relationship.json")
    # Build a master dict
    if not(os.path.exists(concept_relationship_json)):
        print("Generating '%s'" % concept_relationship_json)
        csv_file_name_to_keyed_json(concept_relationship_csv, concept_relationship_json, "CONCEPT_ID_1".lower(),
                                    ("RELATIONSHIP_ID".lower(), "Maps to"))

    with open_csv_file(concept_csv, "r") as f:
        dict_reader = csv.DictReader(f, delimiter=delimiter)
        concept_dict_vocabulary = {}
        for row_dict in dict_reader:
            concept_dict_vocabulary[row_dict["CONCEPT_ID".lower()]] = row_dict["VOCABULARY_ID".lower()]

        global_concept_json = os.path.join(output_json_directory, "global_concept_vocabulary.json")
        print("Generating '%s'" % global_concept_json)
        with open(global_concept_json, "w") as fw:
            json.dump(concept_dict_vocabulary, fw, sort_keys=True, indent=4, separators=(',', ': '))

    with open_csv_file(concept_csv) as f:
        dict_reader = csv.DictReader(f, delimiter=delimiter)
        concept_dict_domain = {}
        for row_dict in dict_reader:
            concept_dict_domain[row_dict["CONCEPT_ID".lower()]] = row_dict["DOMAIN_ID".lower()]

        global_concept_domain_json = os.path.join(output_json_directory, "global_concept_domain.json")
        print("Generating '%s'" % global_concept_json)
        with open(global_concept_domain_json, "w") as fw:
            json.dump(concept_dict_vocabulary, fw, sort_keys=True, indent=4, separators=(',', ': '))

    vocabularies_with_maps = ["ICD9CM", "ICD9Proc", "ICD10CM", "ICD10PCS", "Multum", "LOINC", "CPT4", "HCPCS", "NDC"]
    for vocabulary_id in vocabularies_with_maps:
        print("Annotating '%s'" % vocabulary_id)
        vocabulary_json = os.path.join(output_json_directory, "concept_code_" + vocabulary_id + ".json")

        concept_with_parent_json = os.path.join(output_json_directory, vocabulary_id + "_with_parent.json")

        if not os.path.exists(concept_with_parent_json):
            with open(vocabulary_json, "r") as fj:
                vocabulary_dict = json.load(fj)

            with open(concept_relationship_json, "r") as fj:
                concept_rel_dict = json.load(fj)

            for concept_code in vocabulary_dict:
                concept_dict = vocabulary_dict[concept_code]
                concept_id = concept_dict["CONCEPT_ID".lower()]
                if concept_id in concept_rel_dict:
                    try:
                        mapped_concept_id = concept_rel_dict[concept_id]["CONCEPT_ID_2".lower()]
                    except TypeError:
                        multiple_concepts = concept_rel_dict[concept_id]
                        multiple_concepts.sort(key=lambda x: x["VALID_END_DATE".lower()], reverse=True)
                        mapped_concept_id = multiple_concepts[0]["CONCEPT_ID_2".lower()]

                    concept_dict["MAPPED_CONCEPT_ID".lower()] = mapped_concept_id
                    if mapped_concept_id in concept_dict_vocabulary:
                        concept_dict["MAPPED_CONCEPT_VOCAB".lower()] = concept_dict_vocabulary[mapped_concept_id]
                    else:
                        concept_dict["MAPPED_CONCEPT_VOCAB".lower()] = None

                    if mapped_concept_id in concept_dict_domain:
                        concept_dict["MAPPED_CONCEPT_DOMAIN".lower()] = concept_dict_domain[mapped_concept_id]
                    else:
                        concept_dict["MAPPED_CONCEPT_DOMAIN".lower()] = None

                else:
                    concept_dict["MAPPED_CONCEPT_ID".lower()] = None

            with open(concept_with_parent_json, "w") as fw:
                json.dump(vocabulary_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


def csv_file_name_to_keyed_json(csv_file_name, json_file_name, field_to_key_on, filter_pairs=None, delimiter="\t"):
    """Create a keyed JSON file"""
    with open_csv_file(csv_file_name, "r") as fd:
        dict_reader = csv.DictReader(fd, delimiter=delimiter)
        result_dict = {}

        include_row = True
        filter_value = None
        field = None

        if filter_pairs is not None:
            if filter_pairs.__class__ != [].__class__:
                filter_pairs = [filter_pairs]

            include_row = False

        for row_dict in dict_reader:
            if filter_pairs is not None:
                for filter_pair in filter_pairs:
                    field, filter_value = filter_pair
                    field_value = row_dict[field]
                    if field_value == filter_value:
                        include_row = True
                    else:
                        include_row = False
                        break

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

    arg_parse_obj = argparse.ArgumentParser(
        description="Transform Athena vocabulary files into JSON map files for mapping scripts")

    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="cdm_config.json")
    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as fc:
        config_dict = json.load(fc)

    main(config_dict["json_map_directory"])