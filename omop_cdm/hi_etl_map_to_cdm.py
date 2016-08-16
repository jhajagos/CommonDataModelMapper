
from omop_cdm_functions import *
from omop_cdm_classes import *
from hi_classes import *
import os
from mapping_classes import *


def person_router_obj(input_dict):
    return PersonObject()


def death_router_obj(input_dict):
    "Determines if a row_dict codes a death"
    if input_dict["deceased"] == "true":
        return DeathObject()
    else:
        return NoOutputClass()


def main(input_csv_directory, output_csv_directory, json_map_directory):

    # Provider

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

    person_runner_obj = RunMapperAgainstSingleInputRealization(hi_person_csv_obj, in_out_map_obj, output_directory_obj,
                                                            person_router_obj)
    person_runner_obj.run()

    person_json_file_name = create_json_map_from_csv_file(output_person_csv, "person_source_value", "person_id")
    empi_id_mapper = CoderMapperJSONClass(person_json_file_name)

    death_concept_mapper = ChainMapper(ReplacementMapper({"true": 'EHR record patient status "Deceased"'}),
                                                         CoderMapperJSONClass(os.path.join(json_map_directory,
                                                                                           "CONCEPT_NAME_Death_Type.json")))

    hi_person_death_csv_obj = InputClassCSVRealization(input_person_csv, PHDPersonObject())

    death_rules = [("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                   ("deceased", death_concept_mapper, {"CONCEPT_ID":  "death_type_concept_id"}),
                   ("deceased_dt_tm", SplitDateTimeWithTZ(), {"date": "death_date"})]

    output_death_csv = os.path.join(output_csv_directory, "death_cdm.csv")
    cdm_death_csv_obj = OutputClassCSVRealization(output_death_csv, DeathObject())

    death_mapper_rules_class = build_input_output_mapper(death_rules)

    in_out_map_obj.register(PHDPersonObject(), DeathObject(), death_mapper_rules_class)
    output_directory_obj.register(DeathObject(), cdm_death_csv_obj)

    death_runner_obj = RunMapperAgainstSingleInputRealization(hi_person_death_csv_obj, in_out_map_obj, output_directory_obj,
                                                              death_router_obj)

    death_runner_obj.run()

    # visit_occurrence

    # ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date", "visit_start_time", "visit_end_date", "visit_end_time", "visit_type_concept_id", "provider_id", "care_site_id", "visit_source_value", "visit_source_concept_id"]

    # measurement

    # ["measurement_id", "person_id", "measurement_concept_id", "measurement_date", "measurement_time", "measurement_type_concept_id", "operator_concept_id", "value_as_number", "value_as_concept_id", "unit_concept_id", "range_low", "range_high", "provider_id", "visit_occurrence_id", "measurement_source_value", "measurement_source_concept_id", "unit_source_value", "value_source_value"]

    # condition

    # procedure

    # observation - DRGs

    # drug_exposure


if __name__ == "__main__":
    with open("hi_config.json", "r") as f:
        config_dict = json.load(f)

    main(config_dict["csv_input_directory"], config_dict["csv_output_directory"], config_dict["json_map_directory"])



