"""
Mapping data extracted from a prepared source. A first level mapping from
the sources file has been completed. Writing ETLs for the OMOP CDM is complex
because of the mappings and that a single source file can map to multiple tables.

Fields in the prepared source start either start with s_  for source or m_ mapped
to a name part of the OHDSI vocabulary.
"""

import os
import sys

try:
    from source_to_cdm_functions import *
    from omop_cdm_classes_5_2 import *
    from prepared_source_classes import *
    from mapping_classes import *
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, "src")))
    from source_to_cdm_functions import *
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

    #### Death ####

    # Generate look up for s_person_id
    output_person_csv = os.path.join(output_csv_directory, "person_cdm.csv")
    person_json_file_name = create_json_map_from_csv_file(output_person_csv, "person_source_value", "person_id")
    s_person_id_mapper = CoderMapperJSONClass(person_json_file_name)

    death_rules = create_death_person_rules(json_map_directory, s_person_id_mapper)

    output_death_csv = os.path.join(output_csv_directory, "death_cdm.csv")
    death_runner_obj = generate_mapper_obj(input_person_csv, SourcePersonObject(), output_death_csv, DeathObject(),
                                           death_rules, output_class_obj, in_out_map_obj, death_router_obj)
    death_runner_obj.run()

    #### Observation Period ####

    obs_per_rules = create_observation_period_rules(json_map_directory, s_person_id_mapper)

    output_obs_per_csv = os.path.join(output_csv_directory, "observation_period_cdm.csv")

    input_obs_per_csv = os.path.join(input_csv_directory, "source_observation_period.csv")

    def obs_router_obj(input_dict):
        return ObservationPeriodObject()

    obs_per_runner_obj = generate_mapper_obj(input_obs_per_csv, SourceObservationPeriodObject(), output_obs_per_csv,
                                             ObservationPeriodObject(),
                                             obs_per_rules, output_class_obj, in_out_map_obj, obs_router_obj)
    obs_per_runner_obj.run()


#### RULES ####

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
                     ("s_birth_datetime", DateSplit(),
                      {"year": "year_of_birth", "month": "month_of_birth", "day": "day_of_birth"}),
                     ("s_birth_datetime", "birth_datetime"),
                     ("s_gender", "gender_source_value"),
                     ("m_gender", gender_mapper, {"CONCEPT_ID": "gender_concept_id"}),
                     ("m_gender", gender_mapper, {"CONCEPT_ID": "gender_source_concept_id"}),
                     ("s_race", "race_source_value"),
                     ("m_race", race_mapper, {"CONCEPT_ID": "race_concept_id"}),
                     ("m_race", race_mapper, {"CONCEPT_ID": "race_source_concept_id"}),
                     ("s_ethnicity", "ethnicity_source_value"),
                     ("m_ethnicity", ethnicity_mapper, {"CONCEPT_ID": "ethnicity_concept_id"}),
                     ("m_ethnicity", ethnicity_mapper, {"CONCEPT_ID": "ethnicity_source_concept_id"})]

    return patient_rules


def create_death_person_rules(json_map_directory, s_person_id_mapper):
    """Generate rules for mapping death"""

    death_concept_mapper = ChainMapper(HasNonEmptyValue(), ReplacementMapper({True: 'EHR record patient status "Deceased"'}),
                                       CoderMapperJSONClass(os.path.join(json_map_directory,
                                                                         "CONCEPT_NAME_Death_Type.json")))

    # TODO: cause_concept_id, cause_source_value, cause_source_concept_id
    # Valid Concepts for the cause_concept_id have domain_id='Condition'.

    # Required person_id, death_date, death_type_concept_id
    death_rules = [("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                   ("s_death_datetime", death_concept_mapper, {"CONCEPT_ID": "death_type_concept_id"}),
                   ("s_death_datetime", SplitDateTimeWithTZ(), {"date": "death_date"}),
                   ("s_death_datetime", DateTimeWithTZ(), {"datetime": "death_datetime"})]

    return death_rules


def create_observation_period_rules(json_map_directory, s_person_id_mapper):
    """Generate observation rules"""
    observation_period_mapper = CoderMapperJSONClass(
        os.path.join(json_map_directory, "CONCEPT_NAME_Obs_Period_Type.json"))
    observation_period_constant_mapper = ChainMapper(
        ConstantMapper({"observation_period_type_name": "Period covering healthcare encounters"}),
        observation_period_mapper)

    observation_period_rules = [(":row_id", "observation_period_id"),
                                ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                                ("s_start_observation_datetime", SplitDateTimeWithTZ(),
                                 {"date": "observation_period_start_date"}),
                                ("s_end_observation_datetime", SplitDateTimeWithTZ(),
                                 {"date": "observation_period_end_date"}),
                                (":row_id", observation_period_constant_mapper,
                                 {"CONCEPT_ID": "period_type_concept_id"})
                                ]

    return observation_period_rules


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


#### Routers #####

def person_router_obj(input_dict):
    """Route a person"""
    return PersonObject()


def death_router_obj(input_dict):
    """Determines if a row_dict codes a death"""
    if len(input_dict["s_death_datetime"]):
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