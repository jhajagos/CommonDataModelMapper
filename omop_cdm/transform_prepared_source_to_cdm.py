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

    #### Observation_Period ####

    obs_per_rules = create_observation_period_rules(json_map_directory, s_person_id_mapper)

    output_obs_per_csv = os.path.join(output_csv_directory, "observation_period_cdm.csv")

    input_obs_per_csv = os.path.join(input_csv_directory, "source_observation_period.csv")

    def obs_router_obj(input_dict):
        return ObservationPeriodObject()

    obs_per_runner_obj = generate_mapper_obj(input_obs_per_csv, SourceObservationPeriodObject(), output_obs_per_csv,
                                             ObservationPeriodObject(),
                                             obs_per_rules, output_class_obj, in_out_map_obj, obs_router_obj)
    obs_per_runner_obj.run()

    #### Visit_Occurrence ###
    visit_rules = create_visit_rules(json_map_directory, s_person_id_mapper)
    input_encounter_csv = os.path.join(input_csv_directory, "source_encounter.csv")
    output_visit_occurrence_csv = os.path.join(output_csv_directory, "visit_occurrence_cdm.csv")

    def visit_router_obj(input_dict):
        if len(s_person_id_mapper.map({"s_person_id": input_dict["s_person_id"]})):
            if input_dict["i_exclude"] != '1':
                return VisitOccurrenceObject()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    visit_runner_obj = generate_mapper_obj(input_encounter_csv, SourceEncounterObject(), output_visit_occurrence_csv,
                                           VisitOccurrenceObject(), visit_rules,
                                           output_class_obj, in_out_map_obj, visit_router_obj)

    visit_runner_obj.run()

    # Visit ID Map
    encounter_json_file_name = create_json_map_from_csv_file(output_visit_occurrence_csv, "visit_source_value",
                                                             "visit_occurrence_id")

    encounter_id_mapper = CoderMapperJSONClass(encounter_json_file_name, "encounter_id")

    visit_runner_obj.run()

    # Visit ID Map
    encounter_json_file_name = create_json_map_from_csv_file(output_visit_occurrence_csv, "visit_source_value",
                                                             "visit_occurrence_id")
    encounter_id_mapper = CoderMapperJSONClass(encounter_json_file_name, "s_encounter_id")

    #### MEASUREMENT and OBSERVATION dervived from PH_F_Result ####
    snomed_code_json = os.path.join(json_map_directory, "CONCEPT_CODE_SNOMED.json")
    snomed_code_mapper = CoderMapperJSONClass(snomed_code_json)
    snomed_code_result_mapper = ChainMapper(FilterHasKeyValueMapper(["s_measurement_result_code"]), snomed_code_mapper)

    def measurement_router_obj(input_dict):
        """Determine if the result contains a LOINC code"""
        if len(s_person_id_mapper.map({"s_person_id": input_dict["s_person_id"]})):
            if input_dict["i_exclude"] != "1":
                if len(input_dict["s_measurement_result_code"]):
                    mapped_result_code = snomed_code_result_mapper.map(input_dict)
                    if "CONCEPT_CLASS_ID" in mapped_result_code:
                        if mapped_result_code["DOMAIN_ID"] == "Measurement":
                            return MeasurementObject()
                        elif mapped_result_code["DOMAIN_ID"] == "Observation":
                            return ObservationObject()
                        else:
                            return NoOutputClass()
                    else:
                        return MeasurementObject()
                else:
                    return NoOutputClass()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    snomed_json = os.path.join(json_map_directory, "CONCEPT_NAME_SNOMED.json")  # We don't need the entire SNOMED
    snomed_mapper = CoderMapperJSONClass(snomed_json)

    measurement_rules, observation_measurement_rules = \
        create_measurement_and_observation_rules(json_map_directory, s_person_id_mapper, encounter_id_mapper, snomed_mapper,
                                                 snomed_code_mapper)

    input_result_csv = os.path.join(input_csv_directory, "source_result.csv")
    output_measurement_csv = os.path.join(output_csv_directory, "measurement_encounter_cdm.csv")

    measurement_runner_obj = generate_mapper_obj(input_result_csv, SourceResultObject(), output_measurement_csv,
                                                 MeasurementObject(),
                                                 measurement_rules, output_class_obj, in_out_map_obj,
                                                 measurement_router_obj)

    output_observation_csv = os.path.join(output_csv_directory, "observation_measurement_encounter_cdm.csv")
    register_to_mapper_obj(input_result_csv, SourceResultObject(), output_observation_csv,
                           ObservationObject(), observation_measurement_rules, output_class_obj, in_out_map_obj)

    #measurement_runner_obj.run()


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


def register_to_mapper_obj(input_csv_file_name, input_class_obj, output_csv_file_name, output_class_obj,
                           map_rules_list,
                           output_obj, in_out_map_obj):

    input_csv_class_obj = InputClassCSVRealization(input_csv_file_name, input_class_obj)

    output_csv_class_obj = OutputClassCSVRealization(output_csv_file_name, output_class_obj)

    map_rules_obj = build_input_output_mapper(map_rules_list)

    output_obj.register(output_class_obj, output_csv_class_obj)

    in_out_map_obj.register(input_class_obj, output_class_obj, map_rules_obj)


def create_visit_rules(json_map_directory, s_person_id_mapper):
    """Generate rules for mapping PH_F_Encounter to VisitOccurrence"""

    visit_concept_json = os.path.join(json_map_directory, "CONCEPT_NAME_Visit.json")
    visit_concept_mapper = ChainMapper(
        ReplacementMapper({"Inpatient": "Inpatient Visit", "Emergency": "Emergency Room Visit",
                           "Outpatient": "Outpatient Visit", "Observation": "Emergency Room Visit",
                           "Recurring": "Outpatient Visit", "Preadmit": "Outpatient Visit", "": "Outpatient Visit"
                           }), # Note: there are no observation type
        CoderMapperJSONClass(visit_concept_json))

    visit_concept_type_json = os.path.join(json_map_directory, "CONCEPT_NAME_Visit_Type.json")
    visit_concept_type_mapper = ChainMapper(ConstantMapper({"visit_concept_name": "Visit derived from EHR record"}),
                                            CoderMapperJSONClass(visit_concept_type_json))

    # TODO: Add care site id

    # Required: visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_type_concept_id
    visit_rules = [("s_encounter_id", IdentityMapper(), {"encounter_id": "visit_source_value"}),
                   ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                   (":row_id", "visit_occurrence_id"),
                   ("m_visit_type", CascadeMapper(visit_concept_mapper, ConstantMapper({"CONCEPT_ID": 0})),
                    {"CONCEPT_ID": "visit_concept_id"}),
                   (":row_id", visit_concept_type_mapper, {"CONCEPT_ID": "visit_type_concept_id"}),
                   ("s_visit_start_datetime", SplitDateTimeWithTZ(),
                    {"date": "visit_start_date", "time": "visit_start_time"}),
                   ("s_visit_end_datetime", SplitDateTimeWithTZ(),
                    {"date": "visit_end_date", "time": "visit_end_time"})]

    return visit_rules


def create_measurement_and_observation_rules(json_map_directory, s_person_id_mapper, encounter_id_mapper, snomed_mapper, snomed_code_mapper):
    """Generate rules for mapping PH_F_Result to Measurement"""

    unit_measurement_mapper = snomed_code_mapper

    loinc_json = os.path.join(json_map_directory, "LOINC_with_parent.json")
    loinc_mapper = CoderMapperJSONClass(loinc_json)

    measurement_code_mapper = CascadeMapper(loinc_mapper, snomed_code_mapper, ConstantMapper({"CONCEPT_ID": 0}))

    # TODO: Currently only map "Lab result" add other measurement types "measurement_type_concept_id"

    # TODO: Add operator Concept ID: A foreign key identifier to the predefined Concept in the Standardized Vocabularies reflecting the mathematical operator that is applied to the value_as_number. Operators are <, <=, =, >=, >.

    measurement_type_json = os.path.join(json_map_directory, "CONCEPT_NAME_Meas_Type.json")
    measurement_type_mapper = CoderMapperJSONClass(measurement_type_json)

    value_as_concept_mapper = ChainMapper(FilterHasKeyValueMapper(["norm_codified_value_code", "interpretation_primary_display", "norm_text_value"]),
        CascadeMapper(snomed_code_mapper, ChainMapper(ReplacementMapper({"Abnormal": "Abnormal",
                           "Above absolute high-off instrument scale": "High",
                           "Above high normal": "High",
                           "Below absolute low-off instrument scale": "Low",
                           "Negative": "Negative",
                           "Normal": "Normal",
                           "Positive": "Positive",
                           "Very abnormal": "Abnormal"
                           }), snomed_mapper)))

    measurement_type_chained_mapper = CascadeMapper(ChainMapper(loinc_mapper, FilterHasKeyValueMapper(["CONCEPT_CLASS_ID"]),
                                                                 ReplacementMapper({"Lab Test": "Lab result"}),
                                                                 measurement_type_mapper), ConstantMapper({"CONCEPT_ID": 0}))

    # "Derived value" "From physical examination"  "Lab result"  "Pathology finding"   "Patient reported value"   "Test ordered through EHR"
    # "CONCEPT_CLASS_ID": "Lab Test"

    numeric_coded_mapper = FilterHasKeyValueMapper(["norm_numeric_value", "norm_codified_value_primary_display", "norm_text_value", "result_primary_display"])

    measurement_rules = [(":row_id", "measurement_id"),
                         ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                         ("s_encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                         ("s_measurement_datetime", SplitDateTimeWithTZ(), {"date": "measurement_date", "time": "measurement_time"}),
                         ("result_code", "measurement_source_value"),
                         ("result_code", measurement_code_mapper,  {"CONCEPT_ID": "measurement_source_concept_id"}),
                         ("result_code", measurement_code_mapper,  {"CONCEPT_ID": "measurement_concept_id"}),
                         ("result_code", measurement_type_chained_mapper, {"CONCEPT_ID": "measurement_type_concept_id"}),
                         ("norm_numeric_value", FloatMapper(), "value_as_number"),
                         (("norm_codified_value_code", "interpretation_primary_display", "norm_text_value"),
                          value_as_concept_mapper, {"CONCEPT_ID": "value_as_concept_id"}), #norm_codified_value_primary_display",
                         ("norm_unit_of_measure_primary_display", "unit_source_value"),
                         ("norm_unit_of_measure_code", unit_measurement_mapper, {"CONCEPT_ID": "unit_concept_id"}),
                         (("norm_numeric_value", "norm_codified_value_primary_display", "result_primary_display", "norm_text_value"),
                            numeric_coded_mapper, #ChainMapper(numeric_coded_mapper, LeftMapperString(50)),
                          {"norm_numeric_value": "value_source_value",
                           "norm_codified_value_primary_display": "value_source_value",
                           "result_primary_display": "value_source_value",
                           "norm_text_value": "value_source_value"}),
                         ("norm_ref_range_low", FloatMapper(), "range_low"),  # TODO: Some values contain non-numeric elements
                         ("norm_ref_range_high", FloatMapper(), "range_high")]

    #TODO: observation_type_concept_id <- "Observation recorded from EHR"
    measurement_observation_rules = [(":row_id", "observation_id"),
                                     ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                                     ("s_encounter_id", encounter_id_mapper,
                                      {"visit_occurrence_id": "visit_occurrence_id"}),
                                     ("service_date", SplitDateTimeWithTZ(),
                                      {"date": "observation_date", "time": "observation_time"}),
                                     ("result_code", "observation_source_value"),
                                     ("result_code", measurement_code_mapper,
                                      {"CONCEPT_ID": "observation_source_concept_id"}),
                                     ("result_code", measurement_code_mapper,
                                      {"CONCEPT_ID": "observation_concept_id"}),
                                     ("result_code", measurement_type_chained_mapper,
                                      {"CONCEPT_ID": "observation_type_concept_id"}),
                                     ("norm_numeric_value", FloatMapper(), "value_as_number"),
                                     (("norm_numeric_value", "norm_codified_value_primary_display", "result_primary_display", "norm_text_value"),
                                      value_as_concept_mapper, {"CONCEPT_ID": "value_as_concept_id"}),
                                     ("norm_unit_of_measure_primary_display", "unit_source_value"),
                                     ("norm_unit_of_measure_code", unit_measurement_mapper,
                                      {"CONCEPT_ID": "unit_concept_id"}),
                                     (("norm_numeric_value", "norm_codified_value_primary_display", "result_primary_display",
                                       "norm_text_value"), numeric_coded_mapper,
                                      {"norm_numeric_value": "value_source_value",
                                       "norm_codified_value_primary_display": "value_source_value",
                                       "result_primary_display": "value_source_value",
                                       "norm_text_value": "value_source_value"})
                                     ]

    return measurement_rules, measurement_observation_rules


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