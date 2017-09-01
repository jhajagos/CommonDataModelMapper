"""
Mapping data extracted from HealtheIntent Data warehouse into the OMOP CDM
"""

import os
import sys

try:
    from omop_cdm_functions import *
    from omop_cdm_classes_5_0 import *
    from hi_classes import *
    from mapping_classes import *
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, "src")))
    from omop_cdm_functions import *
    from omop_cdm_classes_5_2 import *
    from hi_classes import *
    from mapping_classes import *

import logging
import csv
import argparse

logging.basicConfig(level=logging.INFO)


def main(input_csv_directory, output_csv_directory, json_map_directory):
    # TODO: Add Provider
    # TODO: Add Patient Location
    # TODO: Handle End Dates

    output_class_obj = OutputClassDirectory()

    in_out_map_obj = InputOutputMapperDirectory()
    output_directory_obj = OutputClassDirectory()

    #### Person ####

    input_person_csv = os.path.join(input_csv_directory, "source_person.csv")
    output_person_csv = os.path.join(output_csv_directory, "person_cdm.csv")

    person_race_csv = os.path.join(input_csv_directory, "source_person_race.csv")
    person_demographic_csv = os.path.join(input_csv_directory, "source_person_ethnicity.csv")

    build_json_person_attribute(person_race_csv, "person_race.json", "s_sequence_id", "s_race_code", "s_race",
                                output_directory=input_csv_directory)

    build_json_person_attribute(person_demographic_csv, "person_ethnicity.json", "s_sequence_id", "s_ethnicity_code",
                                "s_ethnicity",
                                output_directory=input_csv_directory)

    person_race_code_mapper = CoderMapperJSONClass(os.path.join(input_csv_directory, "person_race.json"))

    person_ethnicity_code_mapper = CoderMapperJSONClass(os.path.join(input_csv_directory, "person_ethnicity.json"))

    patient_rules = create_patient_rules(json_map_directory, person_race_code_mapper, person_ethnicity_code_mapper)

    person_runner_obj = generate_mapper_obj(input_person_csv, PHDPersonObject(), output_person_csv, PersonObject(),
                                            patient_rules,
                                            output_class_obj, in_out_map_obj, person_router_obj)

    person_runner_obj.run()


def create_patient_rules(json_map_directory, person_race_mapper, person_ethnicity_mapper):
    """Generate rules for mapping PH_D_Person mapper"""

    gender_json = os.path.join(json_map_directory, "CONCEPT_NAME_Gender.json")
    gender_json_mapper = CoderMapperJSONClass(gender_json)
    upper_case_mapper = TransformMapper(lambda x: x.upper())
    gender_mapper = CascadeMapper(ChainMapper(upper_case_mapper, gender_json_mapper), ConstantMapper({"CONCEPT_ID": 0}))

    race_json = os.path.join(json_map_directory, "CONCEPT_NAME_Race.json")
    race_json_mapper = CoderMapperJSONClass(race_json)

    ethnicity_json = os.path.join(json_map_directory, "CONCEPT_NAME_Ethnicity.json")
    ethnicity_json_mapper = CoderMapperJSONClass(ethnicity_json)

    race_map_dict = {"American Indian or Alaska native": "American Indian or Alaska Native",
                     "Asian or Pacific islander": "Asian",
                     "Black, not of hispanic origin": "Black",
                     "Caucasian": "White",
                     "Indian": "Asian Indian"
                    }

    ethnicity_map_dict = {
        "Hispanic or Latino": "Hispanic or Latino",
        "Not Hispanic or Latino": "Not Hispanic or Latino"
    }

    race_mapper = CascadeMapper(ChainMapper(person_race_mapper,
                              FilterHasKeyValueMapper(["description"]), ReplacementMapper(race_map_dict),
                              race_json_mapper), ConstantMapper({"CONCEPT_ID": 0}))

    ethnicity_mapper = CascadeMapper(ChainMapper(person_ethnicity_mapper,
                              FilterHasKeyValueMapper(["description"]), ReplacementMapper(ethnicity_map_dict),
                              ethnicity_json_mapper), ConstantMapper({"CONCEPT_ID": 0}))

    # TODO: Replace :row_id with starting seed that increments
    # Required person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id
    patient_rules = [(":row_id", row_map_offset("person_id", 0), {"person_id": "person_id"}),
                     ("s_person_id", "person_source_value"),
                     ("birth_date", DateSplit(),
                      {"year": "year_of_birth", "month": "month_of_birth", "day": "day_of_birth"}),
                     ("gender_display", "gender_source_value"),
                     ("gender_display", gender_mapper, {"CONCEPT_ID": "gender_concept_id"}),
                     ("gender_display", gender_mapper, {"CONCEPT_ID": "gender_source_concept_id"}),
                     ("s_person_id", race_mapper, {"CONCEPT_ID": "race_concept_id"}),
                     ("s_person_id", race_mapper, {"CONCEPT_ID": "race_source_concept_id"}),
                     ("s_person_id", person_race_mapper, {"description": "race_source_value"}),
                     ("s_person_id", ethnicity_mapper, {"CONCEPT_ID": "ethnicity_concept_id"}),
                     ("s_person_id", ethnicity_mapper, {"CONCEPT_ID": "ethnicity_source_concept_id"}),
                     ("s_person_id", person_ethnicity_mapper, {"description": "ethnicity_source_value"}),

                     ]

    return patient_rules


def build_json_person_attribute(person_attribute_filename, attribute_json_file_name, sequence_field_name,
                                code_field_name, description_field_name,
                                descriptions_to_ignore=["Other", "Patient data refused", "Unknown",
                                                        "Ethnic group not given - patient refused", ""],
                                output_directory="./"):

    """Due to that a Person can have multiple records for ethnicity and race we need to create a lookup"""

    master_attribute_dict = {}
    with open(person_attribute_filename, "rb") as f:

        csv_dict_reader = csv.DictReader(f)

        for row_dict in csv_dict_reader:
            source_patient_id = row_dict["s_person_id"]
            sequence_id = row_dict[sequence_field_name]
            code = row_dict[code_field_name]
            code_description = row_dict[description_field_name]

            if code_description not in descriptions_to_ignore:

                record_attributes = {"sequence_id": sequence_id, "code": code, "description": code_description}

                if source_patient_id in master_attribute_dict:
                    master_attribute_dict[source_patient_id] += [record_attributes]
                else:
                    master_attribute_dict[source_patient_id] = [record_attributes]

        final_attribute_dict = {}
        for source_patient_id in master_attribute_dict:

            attribute_records = master_attribute_dict[source_patient_id]

            attribute_records.sort(key=lambda x: int(x["sequence_id"]))

            final_attribute_dict[source_patient_id] = attribute_records[0]

        full_attribute_json_file_name = os.path.join(output_directory, attribute_json_file_name)

        with open(full_attribute_json_file_name, "w") as fw:
            json.dump(final_attribute_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


def generate_mapper_obj(input_csv_file_name, input_class_obj, output_csv_file_name, output_class_obj, map_rules_list,
                        output_obj, in_out_map_obj, input_router_func, pre_map_func=None, post_map_func=None):

    input_csv_class_obj = InputClassCSVRealization(input_csv_file_name, input_class_obj)
    output_csv_class_obj = OutputClassCSVRealization(output_csv_file_name, output_class_obj)

    map_rules_obj = build_input_output_mapper(map_rules_list)

    output_obj.register(output_class_obj, output_csv_class_obj)

    in_out_map_obj.register(input_class_obj, output_class_obj, map_rules_obj)

    map_runner_obj = RunMapperAgainstSingleInputRealization(input_csv_class_obj, in_out_map_obj, output_obj,
                                                            input_router_func, pre_map_func, post_map_func)

    return map_runner_obj


### Routers ####
def person_router_obj(input_dict):
    """Route a person"""
    return PersonObject()


def death_router_obj(input_dict):
    """Determines if a row_dict codes a death"""
    if input_dict["deceased"] == "true":
        return DeathObject()
    else:
        return NoOutputClass()


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser()
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file", default="hi_config.json")
    arg_obj = arg_parse_obj.parse_args()


    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as f:
        config_dict = json.load(f)

    main(config_dict["csv_input_directory"], config_dict["csv_output_directory"], config_dict["json_map_directory"])