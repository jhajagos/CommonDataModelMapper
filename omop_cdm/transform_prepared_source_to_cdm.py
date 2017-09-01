"""
Mapping data extracted from a prepared source. A first level mapping from
the sources file has been completed. Writing ETLs for the OMOP CDM is complex
bacause of the mappings and that a single source file can map to multiple tables.

Fields in the prepared source start either start with s_  for source or m_ mapped
to a name part of the OHDSI vocabulary.
"""

import os
import sys

try:
    from omop_cdm_functions import *
    from omop_cdm_classes_5_2 import *
    from prepared_source_classes import *
    from mapping_classes import *
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, "src")))
    from omop_cdm_functions import *
    from omop_cdm_classes_5_2 import *
    from prepared_source_classes import *
    from mapping_classes import *

import logging
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

    person_rules = create_person_rules(json_map_directory)

    person_runner_obj = generate_mapper_obj(input_person_csv, SourcePersonObject(), output_person_csv, PersonObject(),
                                            person_rules,
                                            output_class_obj, in_out_map_obj, person_router_obj)

    person_runner_obj.run()


def create_person_rules(json_map_directory):
    """Generate rules for mapping source_patient.csv"""

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

    race_mapper = CascadeMapper(ChainMapper(
                              FilterHasKeyValueMapper(["m_race"]), ReplacementMapper(race_map_dict),
                              race_json_mapper), ConstantMapper({"CONCEPT_ID": 0}))

    ethnicity_mapper = CascadeMapper(ChainMapper(
                              FilterHasKeyValueMapper(["m_ethnicity"]), ReplacementMapper(ethnicity_map_dict),
                              ethnicity_json_mapper), ConstantMapper({"CONCEPT_ID": 0}))

    # TODO: Replace :row_id with starting seed that increments
    # Required person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id
    patient_rules = [(":row_id", row_map_offset("person_id", 0), {"person_id": "person_id"}),
                     ("s_person_id", "person_source_value"),
                     ("s_date_of_birth", DateSplit(),
                      {"year": "year_of_birth", "month": "month_of_birth", "day": "day_of_birth"}),
                     ("s_date_of_birth", "birth_datetime"),
                     ("s_gender", "gender_source_value"),
                     ("m_gender", gender_mapper, {"CONCEPT_ID": "gender_concept_id"}),
                     ("m_gender", gender_mapper, {"CONCEPT_ID": "gender_source_concept_id"}),
                     ("s_race", "race_source_value"),
                     ("m_race", race_mapper, {"CONCEPT_ID": "race_concept_id"}),
                     ("m_race", race_mapper, {"CONCEPT_ID": "race_source_concept_id"}),
                     ("s_ethnicity", "ethnicity_source_value"),
                     ("m_ethnicity", ethnicity_mapper, {"CONCEPT_ID": "ethnicity_concept_id"}),
                     ("m_ethnicity", ethnicity_mapper, {"CONCEPT_ID": "ethnicity_source_concept_id"})
                    ]

    return patient_rules


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