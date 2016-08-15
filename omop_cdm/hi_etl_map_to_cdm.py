
from omop_cdm_functions import *
from omop_cdm_classes import *
from hi_classes import *
import os
from mapping_classes import *


def person_router_obj(input_dict):
    return PersonObject()


def main(input_csv_directory, output_csv_directory, json_map_directory):

    # Location

    gender_json = os.path.join(json_map_directory, "CONCEPT_NAME_Gender.json")
    gender_json_mapper = CoderMapperJSONClass(gender_json)
    upper_case_mapper = TransformMapper(lambda x: x.upper())
    gender_mapper = ChainMapper(upper_case_mapper, gender_json_mapper)

    # Person input / output

    input_person_csv = os.path.join(input_csv_directory, "PH_D_Person.csv")
    hi_person_csv_obj = InputClassCSVRealization(input_person_csv, PHDPersonObject())

    output_person_csv = os.path.join(output_csv_directory, "person_cdm.csv")
    cdm_person_csv_obj = OutputClassCSVRealization(output_person_csv, PersonObject())

    # time_of_birth
    # race_concept_id
    # ethnicity_concept_id
    # location_id
    # provider_id
    # care_site_id
    # person_source_value
    # gender_source_value
    # gender_source_concept_id

    # Person input mapper

    patient_rules = [(":row_id", "person_id"), ("empi_id", "person_source_value"),
                     ("birth_date", DateSplit(),
                        {"year": "year_of_birth", "month": "month_of_birth", "day": "day_of_birth"}),
                     ("gender_display", "gender_source_value"),
                     ("gender_display", gender_mapper, {"CONCEPT_ID": "gender_concept_id"})
                     ]

    patient_mapper_rules_class = build_input_output_mapper(patient_rules)

    in_out_map_obj = InputOutputMapperDirectory()
    in_out_map_obj.register(PHDPersonObject(), PersonObject(), patient_mapper_rules_class)

    output_directory_obj = OutputClassDirectory()
    output_directory_obj.register(PersonObject(), cdm_person_csv_obj)

    map_runner_obj = RunMapperAgainstSingleInputRealization(hi_person_csv_obj, in_out_map_obj, output_directory_obj,
                                                            person_router_obj)

    map_runner_obj.run()

    #TODO: Implement the following mappings
    # death
    #deceased	deceased_dt_tm	deceased_date_id	deceased_source_type	deceased_source_id	deceased_source_version	deceased_source_description	deceased_reference_id

    # measurement

    # visit_occurrence

    # condition

    # procedure

    # observation

    # drug_exposure




if __name__ == "__main__":
    with open("hi_config.json", "r") as f:
        config_dict = json.load(f)

    main(config_dict["csv_input_directory"], config_dict["csv_output_directory"], config_dict["json_map_directory"])



