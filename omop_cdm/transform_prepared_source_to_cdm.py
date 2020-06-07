"""
Mapping data extracted from a prepared source. A first level mapping from
the sources file has been completed. Writing ETLs for the OMOP CDM is complex
because of the mappings and that a single source file can map to multiple tables.

Fields in the prepared source start either start with s_  for source or m_ mapped
to a name part of the OHDSI vocabulary.
"""

#TODO: Config needs to read timezone information right now defualt to US/Eastern

import os
import sys

try:
    from source_to_cdm_functions import *
    from omop_cdm_classes_5_3 import *
    from prepared_source_classes import *
    from mapping_classes import *
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, "src")))
    from source_to_cdm_functions import *
    from omop_cdm_classes_5_3 import *
    from prepared_source_classes import *
    from mapping_classes import *

import logging
import argparse

logging.basicConfig(level=logging.INFO)


def check_one_to_one_mapping(output_dict, domain):
    if domain + "_source_concept_id" in output_dict:
        if output_dict[domain + "_source_concept_id"] != 0:
            if output_dict[domain + "_concept_id"] == 0:
                output_dict[domain +"_concept_id"] = output_dict[domain + "_source_concept_id"]
    return output_dict


def condition_post_processing(output_dict):
    output_dict = check_one_to_one_mapping(output_dict, "condition")
    output_dict = check_one_to_one_mapping(output_dict, "procedure")
    output_dict = check_one_to_one_mapping(output_dict, "observation")
    output_dict = check_one_to_one_mapping(output_dict, "measurement")
    return output_dict


def procedure_post_processing(output_dict):
    """For concept_id"""
    fields = ["drug_concept_id", "drug_source_concept_id", "procedure_concept_id", "procedure_source_concept_id",
              "measurement_concept_id", "measurement_source_concept_id", "observation_concept_id",
              "observation_source_concept_id"]

    for field in fields:
        if field not in output_dict:
            output_dict[field] = 0
        else:
            if output_dict[field] is not None:
                if not len(output_dict[field]):
                    output_dict[field] = 0
            elif output_dict[field] is None:
                output_dict[field] = 0

    output_dict = check_one_to_one_mapping(output_dict, "procedure")
    output_dict = check_one_to_one_mapping(output_dict, "observation")
    output_dict = check_one_to_one_mapping(output_dict, "measurement")

    return output_dict


def main(input_csv_directory, output_csv_directory, json_map_directory):
    # TODO: Add Provider

    output_class_obj = OutputClassDirectory()
    in_out_map_obj = InputOutputMapperDirectory()
    output_directory_obj = OutputClassDirectory()

    ### Location ###

    input_location_csv = os.path.join(input_csv_directory, "source_location.csv")
    output_location_csv = os.path.join(output_csv_directory, "location_cdm.csv")

    def location_router_obj(input_dict):
        return LocationObject()

    # ["location_id", "address_1", "address_2", "city", "state", "zip", "county", "location_source_value"]
    # k_location,s_address_1,s_address_2,s_city,s_state,s_zip,s_county
    location_rules = [
        (":row_id", "location_id"),
        ("s_address_1", "address_1"),
        ("s_address_2", "address_2"),
        ("s_city", "city"),
        ("s_state", "state"),
        ("s_zip", "zip"),
        ("s_county", "county"),
        ("k_location", "location_source_value")]

    location_runner_obj = generate_mapper_obj(input_location_csv, SourceLocationObject(), output_location_csv,
                                           LocationObject(), location_rules,
                                           output_class_obj, in_out_map_obj, location_router_obj)

    location_runner_obj.run()

    location_json_file_name = create_json_map_from_csv_file(output_location_csv, "location_source_value",
                                                             "location_id")

    k_location_mapper = CoderMapperJSONClass(location_json_file_name, "k_location")

    #### Person ####

    input_person_csv = os.path.join(input_csv_directory, "source_person.csv")
    output_person_csv = os.path.join(output_csv_directory, "person_cdm.csv")

    if os.path.exists(output_person_csv + ".json"):
        premapped_patients_json = output_person_csv + ".json"
    else:
        premapped_patients_json = None

    person_rules = create_person_rules(json_map_directory, k_location_mapper, person_id_json_file_name=premapped_patients_json)

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
        if len(s_person_id_mapper.map({"s_person_id": input_dict["s_person_id"]})):
            return ObservationPeriodObject()
        else:
            return NoOutputClass()

    obs_per_runner_obj = generate_mapper_obj(input_obs_per_csv, SourceObservationPeriodObject(), output_obs_per_csv,
                                             ObservationPeriodObject(),
                                             obs_per_rules, output_class_obj, in_out_map_obj, obs_router_obj)
    obs_per_runner_obj.run()

    #### Care Sites ####

    care_site_rules = [
        (":row_id", "care_site_id"),
        ("s_care_site_name", "care_site_name"),
        ("k_care_site", "care_site_source_value")]

    input_care_site_csv = os.path.join(input_csv_directory, "source_care_site.csv")
    output_care_site_csv = os.path.join(output_csv_directory, "care_site_cdm.csv")

    def care_site_router_obj(input_dict):
        return CareSiteObject()

    care_site_runner_obj = generate_mapper_obj(input_care_site_csv, SourceCareSiteObject(), output_care_site_csv,
                                           CareSiteObject(), care_site_rules,
                                           output_class_obj, in_out_map_obj, care_site_router_obj)

    care_site_runner_obj.run()

    care_site_json_file_name = create_json_map_from_csv_file(output_care_site_csv, "care_site_source_value",
                                                             "care_site_id")

    k_care_site_mapper = CoderMapperJSONClass(care_site_json_file_name, "k_care_site")

    #### Visit_Occurrence ###

    snomed_code_json = os.path.join(json_map_directory, "concept_code_SNOMED.json")
    snomed_code_mapper = CodeMapperClassSqliteJSONClass(snomed_code_json)

    input_encounter_csv = os.path.join(input_csv_directory, "source_encounter.csv")
    output_visit_occurrence_csv = os.path.join(output_csv_directory, "visit_occurrence_cdm.csv")

    if os.path.exists(output_visit_occurrence_csv + ".json"):
        visit_id_json = output_visit_occurrence_csv + ".json"
    else:
        visit_id_json = None

    visit_rules = create_visit_rules(json_map_directory, s_person_id_mapper, k_care_site_mapper, snomed_code_mapper,
                                     visit_id_json)

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

    s_encounter_id_mapper = CoderMapperJSONClass(encounter_json_file_name, "s_encounter_id")

    ### Visit Detail

    input_encounter_detail_csv = os.path.join(input_csv_directory, "source_encounter_detail.csv")
    output_visit_detail_csv = os.path.join(output_csv_directory, "visit_detail_cdm.csv")

    visit_concept_json = os.path.join(json_map_directory, "concept_name_Visit.json")
    visit_concept_mapper = ChainMapper(
        ReplacementMapper({"Inpatient": "Inpatient Visit", "Emergency": "Emergency Room Visit",
                           "Outpatient": "Outpatient Visit", "Observation": "Emergency Room Visit",
                           "Recurring": "Outpatient Visit", "Preadmit": "Outpatient Visit", "": "Outpatient Visit"
                           }),  # Note: there are no Observational status  type
        CoderMapperJSONClass(visit_concept_json))

    visit_concept_type_json = os.path.join(json_map_directory, "concept_name_Visit_Type.json")
    visit_concept_type_mapper = ChainMapper(ConstantMapper({"visit_concept_name": "Visit derived from EHR record"}),
                                            CoderMapperJSONClass(visit_concept_type_json))

    # s_encounter_detail_id, s_person_id, s_encounter_id, s_start_datetime, s_end_datetime, k_care_site,s_visit_detail_type,m_visit_detail_type

    # ["visit_detail_id", "person_id", "visit_detail_concept_id", "visit_start_date",
    #  "visit_start_datetime", "visit_end_date", "visit_end_datetime", "visit_type_concept_id",
    #  "provider_id", "care_site_id", "admitting_source_concept_id", "discharge_to_concept_id",
    #  "preceding_visit_detail_id", "visit_source_value", "visit_source_concept_id", "admitting_source_value",
    #  "discharge_to_source_value", "visit_detail_parent_id", "visit_occurrence_id"]

    visit_detail_rules = [
        (":row_id", "visit_detail_id"),
        ("s_encounter_detail_id", "visit_source_value"),
        ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
        ("s_encounter_id", s_encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
        ("s_start_datetime", SplitDateTimeWithTZ(),
         {"date": "visit_start_date", "time": "visit_start_time"}),
        ("s_start_datetime", DateTimeWithTZ(), {"datetime": "visit_start_datetime"}),
        ("s_end_datetime", SplitDateTimeWithTZ(),
         {"date": "visit_end_date", "time": "visit_end_time"}),
        ("s_end_datetime", DateTimeWithTZ(), {"datetime": "visit_end_datetime"}),
        ("k_care_site", k_care_site_mapper, {"care_site_id": "care_site_id"}),
        (":row_id", visit_concept_type_mapper, "visit_type_concept_id"),
        ("m_visit_detail_type", CascadeMapper(visit_concept_mapper, ConstantMapper({"CONCEPT_ID".lower(): 0})),
         {"CONCEPT_ID".lower(): "visit_detail_concept_id"}),
        (":row_id", ConstantMapper({"visit_type_concept_id": 0}), {"visit_type_concept_id": "visit_type_concept_id"})
    ]

    def visit_detail_router_obj(input_dict):
        # print(input_dict)
        # print(s_person_id_mapper.map({"s_person_id": input_dict["s_person_id"]}))
        if len(s_person_id_mapper.map({"s_person_id": input_dict["s_person_id"]})):
            if len(s_encounter_id_mapper.map({"s_encounter_id": input_dict["s_encounter_id"]})):
                if input_dict["i_exclude"] != '1':
                    return VisitDetailObject()
                else:
                    return NoOutputClass()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    visit_detail_runner_obj = generate_mapper_obj(input_encounter_detail_csv, SourceEncounterDetailObject(),
                                                  output_visit_detail_csv,
                                                  VisitDetailObject(), visit_detail_rules,
                                                  output_class_obj, in_out_map_obj, visit_detail_router_obj)
    visit_detail_runner_obj.run()

    #### Benefit Coverage Period ####

    payer_plan_period_rules = create_payer_plan_period_rules(s_person_id_mapper)

    output_ppp_csv = os.path.join(output_csv_directory, "payer_plan_period_cdm.csv")

    input_ppp_csv = os.path.join(input_csv_directory, "source_encounter_coverage.csv")

    def payer_plan_period_router_obj(input_dict):
        if len(s_person_id_mapper.map({"s_person_id": input_dict["s_person_id"]})):
            return PayerPlanPeriodObject()
        else:
            return NoOutputClass()

    payer_plan_period_runner_obj = generate_mapper_obj(input_ppp_csv, SourceEncounterCoverageObject(), output_ppp_csv,
                                                       PayerPlanPeriodObject(),
                                                       payer_plan_period_rules, output_class_obj, in_out_map_obj,
                                                       payer_plan_period_router_obj
                                                       )
    payer_plan_period_runner_obj.run()

    #### MEASUREMENT and OBSERVATION dervived from 'source_result.csv' ####

    snomed_code_result_mapper = ChainMapper(FilterHasKeyValueMapper(["s_code"]), snomed_code_mapper)

    def measurement_router_obj(input_dict):
        """Determine if the result contains a LOINC code"""
        if len(s_person_id_mapper.map({"s_person_id": input_dict["s_person_id"]})):
            if input_dict["i_exclude"] != "1":
                mapped_result_code = snomed_code_result_mapper.map(input_dict)
                if "CONCEPT_CLASS_ID".lower() in mapped_result_code:
                    if mapped_result_code["DOMAIN_ID".lower()] == "Measurement":
                        return MeasurementObject()
                    elif mapped_result_code["DOMAIN_ID".lower()] == "Observation":
                        return ObservationObject()
                    else:
                        return NoOutputClass()
                else:
                    return MeasurementObject()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    snomed_json = os.path.join(json_map_directory, "concept_name_SNOMED.json")
    snomed_mapper = CodeMapperClassSqliteJSONClass(snomed_json)

    measurement_rules, observation_measurement_rules = \
        create_measurement_and_observation_rules(json_map_directory, s_person_id_mapper, s_encounter_id_mapper, snomed_mapper,
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

    measurement_runner_obj.run()

    #### CONDITION / DX ####

    condition_type_name_json = os.path.join(json_map_directory, "concept_name_Condition_Type.json")
    condition_type_name_map = CoderMapperJSONClass(condition_type_name_json)

    condition_claim_type_map = \
        ChainMapper(
            ReplacementMapper({"Primary": "Primary Condition", "Secondary": "Secondary Condition"}),
            condition_type_name_map
        )

    input_condition_csv = os.path.join(input_csv_directory, "source_condition.csv")
    hi_condition_csv_obj = InputClassCSVRealization(input_condition_csv, SourceConditionObject())

    output_condition_csv = os.path.join(output_csv_directory, "condition_occurrence_dx_cdm.csv")
    cdm_condition_csv_obj = OutputClassCSVRealization(output_condition_csv, ConditionOccurrenceObject())

    icd9cm_json = os.path.join(json_map_directory, "ICD9CM_with_parent.json")
    icd10cm_json = os.path.join(json_map_directory, "ICD10CM_with_parent.json")

    def case_mapper_condition(input_dict, field="m_condition_code_oid"):
        """Map ICD9 and ICD10 to the CDM vocabularies"""
        coding_system_oid = input_dict[field]
        coding_version = condition_coding_system(coding_system_oid)

        if coding_version == "ICD9CM":
            return 0
        elif coding_version == "ICD10CM":
            return 1
        elif coding_version == "SNOMED":
            return 2

    # TODO: condition_type_concept_id
    condition_encounter_mapper = ChainMapper(ConstantMapper({"diagnosis_type_name": "Observation recorded from EHR"}),
                                             condition_type_name_map)

    condition_type_concept_mapper = CascadeMapper(condition_claim_type_map, condition_encounter_mapper)

    def clean_concept_ids(input_dict):

        if "CONCEPT_ID".lower() in input_dict:
            if input_dict["CONCEPT_ID".lower()] == "" or input_dict["CONCEPT_ID".lower()] is None:
                input_dict["CONCEPT_ID".lower()] = 0
        else:
            input_dict["CONCEPT_ID".lower()] = 0

        if "MAPPED_CONCEPT_ID".lower() in input_dict:

            if input_dict["MAPPED_CONCEPT_ID".lower()] == "" or input_dict["MAPPED_CONCEPT_ID".lower()] is None:
                input_dict["MAPPED_CONCEPT_ID".lower()] = 0

        else:
            input_dict["MAPPED_CONCEPT_ID".lower()] = 0

        return input_dict


    ConditionMapper = ChainMapper(CaseMapper(case_mapper_condition,
                            CodeMapperClassSqliteJSONClass(icd9cm_json, "s_condition_code"),
                            CodeMapperClassSqliteJSONClass(icd10cm_json, "s_condition_code"),
                            CodeMapperClassSqliteJSONClass(snomed_code_json, "s_condition_code")),
                            PassThroughFunctionMapper(clean_concept_ids))

    s_condition_type_dict = {"Admitting": "52870002", "Final": "89100005", "Preliminary": "148006"}
    condition_status_snomed_mapper = CodeMapperDictClass(s_condition_type_dict, "s_condition_type")
    condition_status_mapper = ChainMapper(condition_status_snomed_mapper, snomed_code_mapper)


    # Required: condition_occurrence_id, person_id, condition_concept_id, condition_start_date
    condition_rules_dx = [(":row_id", "condition_occurrence_id"),
                          ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                          ("s_encounter_id", s_encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                          (("s_condition_code", "m_condition_code_oid"),
                           ConditionMapper,
                           {"CONCEPT_ID".lower(): "condition_source_concept_id", "MAPPED_CONCEPT_ID".lower(): "condition_concept_id"}),
                          ("s_condition_code", "condition_source_value"),
                          ("m_rank", condition_type_concept_mapper, {"CONCEPT_ID".lower(): "condition_type_concept_id"}),
                          ("s_condition_type", condition_status_mapper, {"CONCEPT_ID".lower(): "condition_status_concept_id"}),
                          ("s_condition_type", "condition_status_source_value"),
                          ("s_start_condition_datetime", SplitDateTimeWithTZ(), {"date": "condition_start_date"}),
                          ("s_start_condition_datetime", DateTimeWithTZ(), {"datetime": "condition_start_datetime"}),
                          ("s_end_condition_datetime", SplitDateTimeWithTZ(), {"date": "condition_end_date"}),
                          ("s_end_condition_datetime", DateTimeWithTZ(), {"datetime": "condition_end_datetime"})
                          ]

    condition_rules_dx_class = build_input_output_mapper(condition_rules_dx)

    # ICD9 and ICD10 conditions which map to measurements according to the CDM Vocabulary
    in_out_map_obj.register(SourceConditionObject(), ConditionOccurrenceObject(), condition_rules_dx_class)
    output_directory_obj.register(ConditionOccurrenceObject(), cdm_condition_csv_obj)

    measurement_row_offset = measurement_runner_obj.rows_run
    measurement_rules_dx = [(":row_id", row_map_offset("measurement_id", measurement_row_offset),
                             {"measurement_id": "measurement_id"}),
                            (":row_id", ConstantMapper({"measurement_type_concept_id": 0}),
                             {"measurement_type_concept_id": "measurement_type_concept_id"}),
                            ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                            ("s_encounter_id", s_encounter_id_mapper,
                             {"visit_occurrence_id": "visit_occurrence_id"}),
                            ("s_start_condition_datetime", SplitDateTimeWithTZ(),
                             {"date": "measurement_date", "time": "measurement_time"}),
                            ("s_start_condition_datetime", DateTimeWithTZ(),
                             {"datetime": "measurement_datetime"}),
                            ("s_condition_code", "measurement_source_value"),
                            (("s_condition_code", "m_condition_code_oid"), ConditionMapper,
                             {"CONCEPT_ID".lower(): "measurement_source_concept_id",
                              "MAPPED_CONCEPT_ID".lower(): "measurement_concept_id"})]

    measurement_rules_dx_class = build_input_output_mapper(measurement_rules_dx)
    in_out_map_obj.register(SourceConditionObject(), MeasurementObject(), measurement_rules_dx_class)

    # The mapped ICD9 to measurements get mapped to a separate code
    output_measurement_dx_encounter_csv = os.path.join(output_csv_directory, "measurement_dx_cdm.csv")
    output_measurement_dx_encounter_csv_obj = OutputClassCSVRealization(output_measurement_dx_encounter_csv,
                                                                        MeasurementObject())

    output_directory_obj.register(MeasurementObject(), output_measurement_dx_encounter_csv_obj)

    observation_row_offset = measurement_runner_obj.rows_run

    # ICD9 and ICD10 codes which map to observations according to the CDM Vocabulary
    observation_rules_dx = [(":row_id", row_map_offset("observation_id", observation_row_offset),
                             {"observation_id": "observation_id"}),
                            (":row_id", ConstantMapper({"observation_type_concept_id": 0}),
                             {"observation_type_concept_id": "observation_type_concept_id"}),
                            ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                            (("s_encounter_id",),
                             s_encounter_id_mapper,
                             {"visit_occurrence_id": "visit_occurrence_id"}),
                            ("s_start_condition_datetime", SplitDateTimeWithTZ(),
                             {"date": "observation_date", "time": "observation_time"}),
                            ("s_start_condition_datetime", DateTimeWithTZ(),
                             {"datetime": "observation_datetime"}),
                            ("s_condition_code", "observation_source_value"),
                            (("s_condition_code", "m_condition_code_oid"), ConditionMapper,
                             {"CONCEPT_ID".lower(): "observation_source_concept_id",
                              "MAPPED_CONCEPT_ID".lower(): "observation_concept_id"}),
                            ("m_rank", condition_claim_type_map,
                             {"CONCEPT_ID".lower(): "condition_type_concept_id"})]

    observation_rules_dx_class = build_input_output_mapper(observation_rules_dx)

    output_observation_dx_encounter_csv = os.path.join(output_csv_directory, "observation_dx_cdm.csv")
    output_observation_dx_encounter_csv_obj = OutputClassCSVRealization(output_observation_dx_encounter_csv,
                                                                        ObservationObject())

    output_directory_obj.register(ObservationObject(), output_observation_dx_encounter_csv_obj)
    in_out_map_obj.register(SourceConditionObject(), ObservationObject(), observation_rules_dx_class)

    procedure_type_json = os.path.join(json_map_directory, "concept_name_Procedure_Type.json")
    procedure_type_mapper = CoderMapperJSONClass(procedure_type_json)

    # ICD9 and ICD10 codes which map to procedures according to the CDM Vocabulary
    # "Procedure recorded as diagnostic code"
    # TODO: Map procedure_type_concept_id

    procedure_rules_dx_encounter = [(":row_id", "procedure_occurrence_id"),
                                    (":row_id",
                                     ChainMapper(ConstantMapper({"name": "Procedure recorded as diagnostic code"}),
                                                 procedure_type_mapper),
                                     {"CONCEPT_ID".lower(): "procedure_type_concept_id"}),
                                    ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                                    ("s_encounter_id", s_encounter_id_mapper,
                                     {"visit_occurrence_id": "visit_occurrence_id"}),
                                    ("s_start_condition_datetime", SplitDateTimeWithTZ(),
                                     {"date": "procedure_date"}),
                                    ("s_start_condition_datetime", DateTimeWithTZ(),
                                     {"datetime": "procedure_datetime"}),
                                    ("s_condition_code", "procedure_source_value"),
                                    (("s_condition_code", "m_condition_code_oid"), ConditionMapper,
                                     {"CONCEPT_ID".lower(): "procedure_source_concept_id",
                                      "MAPPED_CONCEPT_ID".lower(): "procedure_concept_id"})]

    procedure_rules_dx_encounter_class = build_input_output_mapper(procedure_rules_dx_encounter)

    output_procedure_dx_encounter_csv = os.path.join(output_csv_directory, "procedure_dx_cdm.csv")
    output_procedure_dx_encounter_csv_obj = OutputClassCSVRealization(output_procedure_dx_encounter_csv,
                                                                      ProcedureOccurrenceObject())

    output_directory_obj.register(ProcedureOccurrenceObject(), output_procedure_dx_encounter_csv_obj)
    in_out_map_obj.register(SourceConditionObject(), ProcedureOccurrenceObject(), procedure_rules_dx_encounter_class)

    def condition_router_obj(input_dict):
        """ICD9 / ICD10 CM contain codes which could either be a procedure, observation, or measurement"""
        coding_system_oid = input_dict["m_condition_code_oid"]

        if coding_system_oid == "null" or coding_system_oid == "":
            coding_system_oid = None

        if coding_system_oid not in ("2.16.840.1.113883.6.90", "2.16.840.1.113883.6.103", '2.16.840.1.113883.6.96'):
            coding_system_oid = None

        if len(s_person_id_mapper.map({"s_person_id": input_dict["s_person_id"]})):

            if input_dict["i_exclude"] != "1":
                if coding_system_oid:

                    try:
                        result_dict = ConditionMapper.map(input_dict)
                    except TypeError:
                        print(coding_system_oid, input_dict)
                        raise

                    if "MAPPED_CONCEPT_DOMAIN".lower() in result_dict or "DOMAIN_ID".lower() in result_dict:
                        if "MAPPED_CONCEPT_DOMAIN".lower() in result_dict:
                            domain = result_dict["MAPPED_CONCEPT_DOMAIN".lower()]
                        else:
                            domain = result_dict["DOMAIN_ID".lower()]
                    else:
                        domain = ""

                    if result_dict != {}:
                        if domain == "Condition":
                            return ConditionOccurrenceObject()
                        elif domain == "Observation":
                            return ObservationObject()
                        elif domain == "Procedure":
                            return ProcedureOccurrenceObject()
                        elif domain == "Measurement":
                            return MeasurementObject()
                        else:
                            return NoOutputClass()
                    else:
                        return NoOutputClass()
                else:
                    return NoOutputClass()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    condition_runner_obj = RunMapperAgainstSingleInputRealization(hi_condition_csv_obj, in_out_map_obj,
                                                                  output_directory_obj,
                                                                  condition_router_obj,
                                                                  post_map_func=condition_post_processing)

    condition_runner_obj.run()

    # Update needed offsets
    condition_row_offset = condition_runner_obj.rows_run
    procedure_row_offset = condition_runner_obj.rows_run
    measurement_row_offset += condition_row_offset
    observation_row_offset += condition_row_offset

    procedure_rules_encounter = create_procedure_rules(json_map_directory, s_person_id_mapper, s_encounter_id_mapper,
                                                       procedure_row_offset)
    procedure_rule = procedure_rules_encounter[0]
    procedure_code_map = procedure_rule[1]

    procedure_rules_encounter_class = build_input_output_mapper(procedure_rules_encounter)

    input_proc_csv = os.path.join(input_csv_directory, "source_procedure.csv")
    hi_proc_csv_obj = InputClassCSVRealization(input_proc_csv, SourceProcedureObject())

    in_out_map_obj.register(SourceProcedureObject(), ProcedureOccurrenceObject(), procedure_rules_encounter_class)

    output_proc_encounter_csv = os.path.join(output_csv_directory, "procedure_cdm.csv")
    output_proc_encounter_csv_obj = OutputClassCSVRealization(output_proc_encounter_csv,
                                                              ProcedureOccurrenceObject())

    output_directory_obj.register(ProcedureOccurrenceObject(), output_proc_encounter_csv_obj)

    #### Measurements from Procedures #####
    measurement_rules_proc_encounter = [(":row_id", row_map_offset("measurement_id", measurement_row_offset),
                                         {"measurement_id": "measurement_id"}),
                                        ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                                        (":row_id", ConstantMapper({"measurement_type_concept_id": 0}),
                                         # TODO: Add measurement_type_concept_id
                                         {"measurement_type_concept_id": "measurement_type_concept_id"}),
                                        ("s_encounter_id", s_encounter_id_mapper,
                                         {"visit_occurrence_id": "visit_occurrence_id"}),
                                        ("s_start_procedure_datetime", SplitDateTimeWithTZ(),
                                         {"date": "measurement_date", "time": "measurement_time"}),
                                        ("s_start_procedure_datetime", DateTimeWithTZ(),
                                         {"datetime": "measurement_datetime"}),
                                        ("s_procedure_code", "measurement_source_value"),
                                        (("s_procedure_code", "m_procedure_code_oid"), procedure_code_map,
                                         {"CONCEPT_ID".lower(): "measurement_source_concept_id",
                                          "MAPPED_CONCEPT_ID".lower(): "measurement_concept_id"})]

    measurement_rules_proc_encounter_class = build_input_output_mapper(measurement_rules_proc_encounter)

    output_measurement_proc_encounter_csv = os.path.join(output_csv_directory, "measurement_proc_cdm.csv")
    output_measurement_proc_encounter_csv_obj = OutputClassCSVRealization(output_measurement_proc_encounter_csv,
                                                                          MeasurementObject())

    output_directory_obj.register(MeasurementObject(), output_measurement_proc_encounter_csv_obj)

    in_out_map_obj.register(SourceProcedureObject(), MeasurementObject(), measurement_rules_proc_encounter_class)

    #### Observations from Procedures #####
    observation_rules_proc = [(":row_id", row_map_offset("observation_id", observation_row_offset),
                               {"observation_id": "observation_id"}),
                              (":row_id", ConstantMapper({"observation_type_concept_id": 0}),
                               {"observation_type_concept_id": "observation_type_concept_id"}),
                              ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                              ("s_encounter_id",
                               s_encounter_id_mapper,
                               {"visit_occurrence_id": "visit_occurrence_id"}),
                              ("s_start_procedure_datetime", SplitDateTimeWithTZ(),
                               {"date": "observation_date", "time": "observation_time"}),
                              ("s_procedure_code", "observation_source_value"),
                              ("s_start_procedure_datetime", DateTimeWithTZ(), {"datetime": "observation_datetime"}),
                              (("s_procedure_code", "m_procedure_code_oid"), procedure_code_map,
                               {"CONCEPT_ID".lower(): "observation_source_concept_id",
                                "MAPPED_CONCEPT_ID".lower(): "observation_concept_id"})]

    observation_rules_proc_class = build_input_output_mapper(observation_rules_proc)
    output_observation_proc_csv = os.path.join(output_csv_directory, "observation_proc_cdm.csv")
    output_observation_proc_csv_obj = OutputClassCSVRealization(output_observation_proc_csv,
                                                                ObservationObject())

    output_directory_obj.register(ObservationObject(), output_observation_proc_csv_obj)
    in_out_map_obj.register(SourceProcedureObject(), ObservationObject(), observation_rules_proc_class)

    ##### DrugExposure from Procedures ####
    drug_rules_proc = [(":row_id", "drug_exposure_id"),
                       (":row_id", ConstantMapper({"drug_type_concept_id": 0}),
                        {"drug_type_concept_id": "drug_type_concept_id"}),
                       ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                       ("s_encounter_id", s_encounter_id_mapper,
                        {"visit_occurrence_id": "visit_occurrence_id"}),
                       ("s_start_procedure_datetime", SplitDateTimeWithTZ(),
                        {"date": "drug_exposure_start_date"}),
                       ("s_start_procedure_datetime", SplitDateTimeWithTZ(),
                        {"date": "drug_exposure_end_date"}),
                       ("s_start_procedure_datetime", DateTimeWithTZ(), {"datetime": "drug_exposure_start_datetime"}),
                       ("s_start_procedure_datetime", DateTimeWithTZ(), {"datetime": "drug_exposure_end_datetime"}),
                       ("s_procedure_code", "drug_source_value"),
                       (("s_procedure_code", "m_procedure_code_oid"), procedure_code_map,
                        {"CONCEPT_ID".lower(): "drug_source_concept_id",
                         "MAPPED_CONCEPT_ID".lower(): "drug_concept_id"})]

    drug_rules_proc_class = build_input_output_mapper(drug_rules_proc)
    output_drug_proc_csv = os.path.join(output_csv_directory, "drug_exposure_proc_cdm.csv")
    output_drug_proc_csv_obj = OutputClassCSVRealization(output_drug_proc_csv,
                                                         DrugExposureObject())

    output_directory_obj.register(DrugExposureObject(), output_drug_proc_csv_obj)
    in_out_map_obj.register(SourceProcedureObject(), DrugExposureObject(), drug_rules_proc_class)

    #### Device Exposure from Procedures ####

    device_rules_proc = [(":row_id", "device_exposure_id"),
                         (":row_id", ConstantMapper({"device_type_concept_id": 0}),
                          {"device_type_concept_id": "device_type_concept_id"}),
                         ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                         ("s_encounter_id",
                          s_encounter_id_mapper,
                          {"visit_occurrence_id": "visit_occurrence_id"}),
                         ("s_start_procedure_datetime", SplitDateTimeWithTZ(),
                          {"date": "device_exposure_start_date"}),
                         ("s_start_procedure_datetime", DateTimeWithTZ(),
                          {"datetime": "device_exposure_start_datetime"}),
                         ("s_procedure_code", "device_source_value"),
                         (("s_procedure_code", "m_procedure_code_oid"), procedure_code_map,
                          {"CONCEPT_ID".lower(): "device_source_concept_id",
                           "MAPPED_CONCEPT_ID".lower(): "device_concept_id"})]

    device_rules_proc_class = build_input_output_mapper(device_rules_proc)
    output_device_proc_csv = os.path.join(output_csv_directory, "device_exposure_proc_cdm.csv")
    output_device_proc_csv_obj = OutputClassCSVRealization(output_device_proc_csv,
                                                           DeviceExposureObject())

    output_directory_obj.register(DeviceExposureObject(), output_device_proc_csv_obj)
    in_out_map_obj.register(SourceProcedureObject(), DeviceExposureObject(), device_rules_proc_class)

    def procedure_router_obj(input_dict):
        if len(s_person_id_mapper.map({"s_person_id": input_dict["s_person_id"]})):

            if "m_procedure_code_oid" in input_dict:

                if procedure_coding_system(input_dict) in ("ICD9 Procedure Codes", "ICD10 Procedure Codes", "CPT Codes",
                                                           "HCPCS", "SNOMED"):

                    result_dict = procedure_code_map.map(input_dict)

                    if "MAPPED_CONCEPT_DOMAIN".lower() in result_dict or "DOMAIN_ID".lower() in result_dict:
                        if "MAPPED_CONCEPT_DOMAIN".lower() in result_dict:
                            domain = result_dict["MAPPED_CONCEPT_DOMAIN".lower()]
                        else:
                            domain = result_dict["DOMAIN_ID".lower()]

                        if domain == "Procedure":
                            return ProcedureOccurrenceObject()
                        elif domain == "Measurement":
                            return MeasurementObject()
                        elif domain == "Observation":
                            return ObservationObject()
                        elif domain == "Drug":
                            return DrugExposureObject()
                        elif domain == "Device":
                            return DeviceExposureObject()
                        else:
                            return NoOutputClass()
                    else:
                        return NoOutputClass()
                else:
                    return NoOutputClass()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    procedure_runner_obj = RunMapperAgainstSingleInputRealization(hi_proc_csv_obj, in_out_map_obj,
                                                                  output_directory_obj,
                                                                  procedure_router_obj, post_map_func=procedure_post_processing)

    procedure_runner_obj.run()

    drug_row_offset = procedure_runner_obj.rows_run

    #### DRUG EXPOSURE ####
    def drug_exposure_router_obj(input_dict):
        """Route mapping of drug_exposure"""

        if len(s_person_id_mapper.map({"s_person_id": input_dict["s_person_id"]})):
            if input_dict["i_exclude"] != "1":
                return DrugExposureObject()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    input_med_csv = os.path.join(input_csv_directory, "source_medication.csv")
    output_drug_exposure_csv = os.path.join(output_csv_directory, "drug_exposure_cdm.csv")

    medication_rules = create_medication_rules(json_map_directory, s_person_id_mapper, s_encounter_id_mapper,
                                               snomed_mapper, drug_row_offset)  # TODO: add drug_row_offset

    drug_exposure_runner_obj = generate_mapper_obj(input_med_csv, SourceMedicationObject(), output_drug_exposure_csv,
                                                   DrugExposureObject(),
                                                   medication_rules, output_class_obj, in_out_map_obj,
                                                   drug_exposure_router_obj, post_map_func=procedure_post_processing)
    drug_exposure_runner_obj.run()


#### RULES ####

def create_person_rules(json_map_directory, k_location_mapper, person_id_json_file_name):
    """Generate rules for mapping source_patient.csv"""

    gender_json = os.path.join(json_map_directory, "concept_name_Gender.json")
    gender_json_mapper = CoderMapperJSONClass(gender_json)
    upper_case_mapper = TransformMapper(lambda x: x.upper())
    gender_mapper = CascadeMapper(ChainMapper(upper_case_mapper,
                                              SingleMatchAddValueMapper(("m_gender", "M"), ("m_gender", "MALE")),
                                              SingleMatchAddValueMapper(("m_gender", "F"), ("m_gender", "FEMALE")),
                                              gender_json_mapper), ConstantMapper({"CONCEPT_ID".lower(): 0}))

    race_json = os.path.join(json_map_directory, "concept_name_Race.json")
    race_json_mapper = CoderMapperJSONClass(race_json)

    race_code_json = os.path.join(json_map_directory, "concept_code_Race.json")
    race_code_mapper = CoderMapperJSONClass(race_code_json, "s_race")

    ethnicity_json = os.path.join(json_map_directory, "concept_name_Ethnicity.json")
    ethnicity_json_mapper = CoderMapperJSONClass(ethnicity_json)

    race_map_dict = {"American Indian or Alaska native": "American Indian or Alaska Native",
                     "Asian or Pacific islander": "Asian",
                     "Black, not of hispanic origin": "Black",
                     "Caucasian": "White",
                     "Indian": "Asian Indian",
                     "White/Caucasian": "White",
                     "Black / African American": "Black",
                     "Black or African American": "Black",
                     "Black/African American": "Black"
                     }

    ethnicity_map_dict = {
        "Colombian": "Hispanic or Latino",
        "Cuban": "Hispanic or Latino",
        "Dominican": "Hispanic or Latino",
        "Ecuadorian": "Hispanic or Latino",
        "Guatemalan": "Hispanic or Latino",
        "Honduran": "Hispanic or Latino",
        "Latin American": "Hispanic or Latino",
        "Mexican": "Hispanic or Latino",
        "Mexican American": "Hispanic or Latino",
        "Mexicano": "Hispanic or Latino",
        "Nicaraguan": "Hispanic or Latino",
        "Paraguayan": "Hispanic or Latino",
        "Peruvian": "Hispanic or Latino",
        "Puerto Rican": "Hispanic or Latino",
        "Salvadoran": "Hispanic or Latino",
        "South American": "Hispanic or Latino",
        "Uruguayan": "Hispanic or Latino",
        "Venezuelan": "Hispanic or Latino",
        "Hispanic or Latino": "Hispanic or Latino",
        "Not Hispanic or Latino": "Not Hispanic or Latino"
    }

    race_mapper = CascadeMapper(
        ChainMapper(FilterHasKeyValueMapper(["s_race"]), race_code_mapper),
        ChainMapper(
            SingleMatchOnlyValueMapper(("m_race", "Other"), ("concept_id", 8522))),
        ChainMapper(
            FilterHasKeyValueMapper(["m_race"]), ReplacementMapper(race_map_dict),
                race_json_mapper),
        ConstantMapper({"CONCEPT_ID".lower(): 0}))

    ethnicity_mapper = CascadeMapper(ChainMapper(
                              FilterHasKeyValueMapper(["m_ethnicity"]), ReplacementMapper(ethnicity_map_dict),
                              ethnicity_json_mapper), ConstantMapper({"CONCEPT_ID".lower(): 0}))

    if person_id_json_file_name is None:
        patient_mapper = row_map_offset("person_id", 0)
    else:
        with open(person_id_json_file_name) as f:
            person_id_dict = json.load(f)
            maximum_patient_id = 1
            for person_id in person_id_dict:
                maximum_patient_id = max(maximum_patient_id, int(person_id_dict[person_id]["person_id"]))

            def person_id_function(item_dict):
                s_person_id = item_dict["s_person_id"]
                if s_person_id in person_id_dict:
                    return {"person_id": person_id_dict[s_person_id]["person_id"]}
                else:
                    return {"person_id": int(item_dict[":row_id"]) + maximum_patient_id + 1}

            patient_mapper = PassThroughFunctionMapper(person_id_function)


    # Required person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id
    patient_rules = [((":row_id", "s_person_id"), patient_mapper, {"person_id": "person_id"}),
                     ("s_person_id", "person_source_value"),
                     ("s_birth_datetime", DateSplit(),
                      {"year": "year_of_birth", "month": "month_of_birth", "day": "day_of_birth"}),
                     ("s_birth_datetime", DateTimeWithTZ(), {"datetime": "birth_datetime"}),
                     ("s_gender", "gender_source_value"),
                     ("m_gender", gender_mapper, {"CONCEPT_ID".lower(): "gender_concept_id"}),
                     ("m_gender", gender_mapper, {"CONCEPT_ID".lower(): "gender_source_concept_id"}),
                     ("s_race", "race_source_value"),
                     (("m_race", "s_race"), race_mapper, {"CONCEPT_ID".lower(): "race_concept_id"}),
                     ("m_race", race_mapper, {"CONCEPT_ID".lower(): "race_source_concept_id"}),
                     ("s_ethnicity", "ethnicity_source_value"),
                     ("m_ethnicity", ethnicity_mapper, {"CONCEPT_ID".lower(): "ethnicity_concept_id"}),
                     ("m_ethnicity", ethnicity_mapper, {"CONCEPT_ID".lower(): "ethnicity_source_concept_id"}),
                     ("k_location", k_location_mapper, {"location_id": "location_id"})
                     ]

    return patient_rules


# Functions for determining coding system
def condition_coding_system(coding_system_oid):
    """Determine from the OID the coding system for conditions"""
    if coding_system_oid == "2.16.840.1.113883.6.90":
        return "ICD10CM"
    elif coding_system_oid == "2.16.840.1.113883.6.103":
        return "ICD9CM"
    elif coding_system_oid == '2.16.840.1.113883.6.96':
        return 'SNOMED'
    else:
        return False


def create_death_person_rules(json_map_directory, s_person_id_mapper):
    """Generate rules for mapping death"""

    death_concept_mapper = ChainMapper(HasNonEmptyValue(), ReplacementMapper({True: 'EHR record patient status "Deceased"'}),
                                       CoderMapperJSONClass(os.path.join(json_map_directory,
                                                                         "concept_name_Death_Type.json")))

    # TODO: cause_concept_id, cause_source_value, cause_source_concept_id
    # Valid Concepts for the cause_concept_id have domain_id='Condition'.

    # Required person_id, death_date, death_type_concept_id
    death_rules = [("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                   ("s_death_datetime", death_concept_mapper, {"CONCEPT_ID".lower(): "death_type_concept_id"}),
                   ("s_death_datetime", SplitDateTimeWithTZ(), {"date": "death_date"}),
                   ("s_death_datetime", DateTimeWithTZ(), {"datetime": "death_datetime"})]

    return death_rules


def create_observation_period_rules(json_map_directory, s_person_id_mapper):
    """Generate observation rules"""
    observation_period_mapper = CoderMapperJSONClass(
        os.path.join(json_map_directory, "concept_name_Obs_Period_Type.json"))
    observation_period_constant_mapper = ChainMapper(
        ConstantMapper({"observation_period_type_name": "Period covering healthcare encounters"}),
        observation_period_mapper)

    observation_period_rules = [(":row_id", "observation_period_id"),
                                ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                                ("s_start_observation_datetime", SplitDateTimeWithTZ(),
                                 {"date": "observation_period_start_date"}),
                                #("s_start_observation_datetime", DateTimeWithTZ(),
                                #  {"datetime": "observation_period_start_datetime"}),
                                ("s_end_observation_datetime", SplitDateTimeWithTZ(),
                                 {"date": "observation_period_end_date"}),
                                # ("s_end_observation_datetime", DateTimeWithTZ(),
                                #  {"datetime": "observation_period_end_datetime"}),
                                (":row_id", observation_period_constant_mapper,
                                 {"CONCEPT_ID".lower(): "period_type_concept_id"})
                                ]

    return observation_period_rules


def create_payer_plan_period_rules(s_person_id_mapper):

    payer_plan_period_rules = [
        (":row_id", "payer_plan_period_id"),
        ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
        ("s_start_payer_date", SplitDateTimeWithTZ(), {"date": "payer_plan_period_start_date"}),
        ("s_end_payer_date", SplitDateTimeWithTZ(), {"date": "payer_plan_period_end_date"}),
        ("m_payer_name", "plan_source_value"),
        ("m_plan_name", "plan_source_value")
    ]

    return payer_plan_period_rules


def procedure_coding_system(input_dict, field="m_procedure_code_oid"):
    """Determine from the OID in procedure coding system"""
    coding_system_oid = input_dict[field]

    if coding_system_oid == '2.16.840.1.113883.6.104':
        return 'ICD9 Procedure Codes'
    elif coding_system_oid == '2.16.840.1.113883.6.12':
        return 'CPT Codes'
    elif coding_system_oid == '2.16.840.1.113883.6.14':
        return 'HCFA Procedure Codes'
    elif coding_system_oid == '2.16.840.1.113883.6.4':
        return 'ICD10 Procedure Codes'
    elif coding_system_oid == '2.16.840.1.113883.6.96':
        return 'SNOMED'
    elif coding_system_oid == '2.16.840.1.113883.6.285':
        return 'HCPCS'
    else:
        return False


def case_mapper_procedures(input_dict, field="m_procedure_code_oid"):
    proc_code_oid = procedure_coding_system(input_dict, field=field)

    if proc_code_oid == "ICD9 Procedure Codes":
        return 0
    elif proc_code_oid == "ICD10 Procedure Codes":
        return 1
    elif proc_code_oid == "CPT Codes":
        return 2
    elif proc_code_oid == "HCPCS":
        return 3
    elif proc_code_oid == "SNOMED":
        return 4



def create_procedure_rules(json_map_directory, s_person_id_mapper, s_encounter_id_mapper, procedure_id_start):
    # Maps the DXs linked by the claims
    # procedure
    # 2.16.840.1.113883.6.104 -- ICD9 Procedure Codes
    # 2.16.840.1.113883.6.12  -- CPT Codes
    # 2.16.840.1.113883.6.14  -- HCFA Procedure Codes
    # 2.16.840.1.113883.6.4 -- ICD10 Procedure Codes
    # 2.16.840.1.113883.6.96 -- SNOMED

    icd9proc_json = os.path.join(json_map_directory, "ICD9Proc_with_parent.json")
    icd10proc_json = os.path.join(json_map_directory, "ICD10PCS_with_parent.json")
    cpt_json = os.path.join(json_map_directory, "CPT4_with_parent.json")
    hcpcs_json = os.path.join(json_map_directory, "HCPCS_with_parent.json")
    snomed_json = os.path.join(json_map_directory, "concept_code_SNOMED.json")
    procedure_type_name_json = os.path.join(json_map_directory, "concept_name_Procedure_Type.json")

    procedure_type_map = \
        CascadeMapper(ChainMapper(
                ReplacementMapper({"PRIMARY": "Primary Procedure", "SECONDARY": "Secondary Procedure"}),
                CoderMapperJSONClass(procedure_type_name_json)),
            ConstantMapper({"CONCEPT_ID".lower(): 0})
        )

    # TODO: Add SNOMED Codes to the Mapping
    ProcedureCodeMapper = CascadeMapper(CaseMapper(case_mapper_procedures,
                                                 CodeMapperClassSqliteJSONClass(icd9proc_json, "s_procedure_code"),
                                                   CodeMapperClassSqliteJSONClass(icd10proc_json, "s_procedure_code"),
                                                   CodeMapperClassSqliteJSONClass(cpt_json, "s_procedure_code"),
                                                   CodeMapperClassSqliteJSONClass(hcpcs_json, "s_procedure_code"),
                                                   CodeMapperClassSqliteJSONClass(snomed_json, "s_procedure_code"),
                                                  ), ConstantMapper({"CONCEPT_ID".lower(): 0, "MAPPED_CONCEPT_ID": 0}))

    # Required: procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id
    procedure_rules_encounter = [(("s_procedure_code", "m_procedure_code_oid"), ProcedureCodeMapper,
                                 {"CONCEPT_ID".lower(): "procedure_source_concept_id",
                                  "MAPPED_CONCEPT_ID".lower(): "procedure_concept_id"}),
                                (":row_id", row_map_offset("procedure_occurrence_id", procedure_id_start),
                                  {"procedure_occurrence_id": "procedure_occurrence_id"}),
                                 ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                                 ("s_encounter_id", s_encounter_id_mapper,
                                  {"visit_occurrence_id": "visit_occurrence_id"}),
                                 ("s_start_procedure_datetime", SplitDateTimeWithTZ(),
                                  {"date": "procedure_date"}),
                                 ("s_start_procedure_datetime", DateTimeWithTZ(), {"datetime": "procedure_datetime"}),
                                 ("s_procedure_code", "procedure_source_value"),
                                 ("s_rank", procedure_type_map, {"CONCEPT_ID".lower(): "procedure_type_concept_id"})]

    return procedure_rules_encounter


def create_visit_rules(json_map_directory, s_person_id_mapper, k_care_site_mapper, snomed_code_mapper, visit_occurrence_id_json_file_name):
    """Generate rules for mapping PH_F_Encounter to VisitOccurrence"""

    visit_concept_json = os.path.join(json_map_directory, "concept_name_Visit.json")
    visit_concept_mapper = ChainMapper(
        ReplacementMapper({"Inpatient": "Inpatient Visit", "Emergency": "Emergency Room Visit",
                           "Outpatient": "Outpatient Visit", "Observation": "Emergency Room Visit",
                           "Recurring": "Outpatient Visit", "Preadmit": "Outpatient Visit", "": "Outpatient Visit"
                           }),  # Note: there are no Observational status  type
        CoderMapperJSONClass(visit_concept_json))

    visit_concept_type_json = os.path.join(json_map_directory, "concept_name_Visit_Type.json")
    visit_concept_type_mapper = ChainMapper(ConstantMapper({"visit_concept_name": "Visit derived from EHR record"}),
                                            CoderMapperJSONClass(visit_concept_type_json))

    place_of_service_json_name = os.path.join(json_map_directory, "concept_name_Place_of_Service.json")
    if not os.path.exists(place_of_service_json_name):
        place_of_service_json_name = os.path.join(json_map_directory, "concept_name_CMS_Place_of_Service.json")

    place_of_service_name_mapper = CoderMapperJSONClass(place_of_service_json_name)
    admit_discharge_source_mapper = CascadeMapper(place_of_service_name_mapper, snomed_code_mapper) # Checks POS then goes to a SNOMED code

    if visit_occurrence_id_json_file_name is None:
        visit_id_mapper = row_map_offset("visit_occurrence_id", 0)
    else:
        with open(visit_occurrence_id_json_file_name) as f:
            visit_id_dict = json.load(f)
            maximum_visit_occurrence_id = 1
            for s_encounter_id in visit_id_dict:
                maximum_visit_occurrence_id = max(maximum_visit_occurrence_id, int(visit_id_dict[s_encounter_id]["visit_occurrence_id"]))

            def visit_id_function(item_dict):
                s_encounter_id = item_dict["s_encounter_id"]
                if s_encounter_id in visit_id_dict:
                    return {"visit_occurrence_id": visit_id_dict[s_encounter_id]["visit_occurrence_id"]}
                else:
                    return {"visit_occurrence_id": int(item_dict[":row_id"]) + 1 + maximum_visit_occurrence_id}

            visit_id_mapper = PassThroughFunctionMapper(visit_id_function)

    # Required: visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_type_concept_id
    visit_rules = [("s_encounter_id", "visit_source_value"),
                   ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                   ((":row_id", "s_encounter_id"), visit_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                   ("m_visit_type", CascadeMapper(visit_concept_mapper, ConstantMapper({"CONCEPT_ID".lower(): 0})),
                    {"CONCEPT_ID".lower(): "visit_concept_id"}),
                   (":row_id", visit_concept_type_mapper, {"CONCEPT_ID".lower(): "visit_type_concept_id"}),
                   ("s_visit_start_datetime", SplitDateTimeWithTZ(),
                    {"date": "visit_start_date", "time": "visit_start_time"}),
                   ("s_visit_start_datetime", DateTimeWithTZ(), {"datetime": "visit_start_datetime"}),
                   ("s_visit_end_datetime", SplitDateTimeWithTZ(),
                    {"date": "visit_end_date", "time": "visit_end_time"}),
                   ("s_visit_end_datetime", DateTimeWithTZ(), {"datetime": "visit_end_datetime"}),
                   ("s_admitting_source", "admitting_source_value"),
                   ("m_admitting_source", admit_discharge_source_mapper, {"CONCEPT_ID".lower(): "admitting_source_concept_id"}),
                   ("s_discharge_to", "discharge_to_source_value"),
                   ("m_discharge_to", admit_discharge_source_mapper, {"CONCEPT_ID".lower(): "discharge_to_concept_id"}),
                   ("k_care_site", k_care_site_mapper, {"care_site_id": "care_site_id"})]

    return visit_rules


def create_measurement_and_observation_rules(json_map_directory, s_person_id_mapper, s_encounter_id_mapper, snomed_mapper, snomed_code_mapper):
    """Generate rules for mapping PH_F_Result to Measurement"""

    ucum_json = os.path.join(json_map_directory, "concept_code_UCUM.json")
    ucum_mapper = CodeMapperClassSqliteJSONClass(ucum_json, "s_result_unit")

    unit_measurement_mapper = CascadeMapper(snomed_code_mapper, ucum_mapper) # Match on SNOMED ID first then try UCUM for the code

    loinc_json = os.path.join(json_map_directory, "LOINC_with_parent.json")
    loinc_mapper = CodeMapperClassSqliteJSONClass(loinc_json)

    NumericMapperConvertDate = CascadeMapper(FloatMapper(), ChainMapper(DateTimeWithTZ("s_result_datetime"),
                                                                        MapDateTimeToUnixEpochSeconds()))

    measurement_code_mapper = CascadeMapper(loinc_mapper, snomed_code_mapper, ConstantMapper({"CONCEPT_ID".lower(): 0}))

    # TODO: Add operator Concept ID: A foreign key identifier to the predefined Concept in the Standardized Vocabularies
    # reflecting the mathematical operator that is applied to the value_as_number. Operators are <, <=, =, >=, >.

    measurement_type_json = os.path.join(json_map_directory, "concept_name_Meas_Type.json")
    measurement_type_mapper = CoderMapperJSONClass(measurement_type_json)

    value_as_concept_mapper = ChainMapper(FilterHasKeyValueMapper(["s_result_code", "m_result_text"]),
        CascadeMapper(snomed_code_mapper, ChainMapper(ReplacementMapper({"Abnormal": "Abnormal",
                           "Above absolute high-off instrument scale": "High",
                           "Above high normal": "High",
                           "Below absolute low-off instrument scale": "Low",
                           "Negative": "Negative",
                           "Normal": "Normal",
                           "Positive": "Positive",
                           "Very abnormal": "Abnormal",
                           "Below low normal": "Low"
                           }), snomed_mapper)))

    measurement_type_chained_mapper = CascadeMapper(ChainMapper(loinc_mapper, FilterHasKeyValueMapper(["CONCEPT_CLASS_ID"]),
                                                                 ReplacementMapper({"Lab Test": "Lab result"}),
                                                                 measurement_type_mapper), ConstantMapper({"CONCEPT_ID".lower(): 0}))

    value_source_mapper = FilterHasKeyValueMapper(["s_result_numeric", "m_result_text", "s_result_datetime", "s_result_code"])

    measurement_rules = [(":row_id", "measurement_id"),
                         ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                         ("s_encounter_id", s_encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                         ("s_obtained_datetime", DateTimeWithTZ(), {"datetime": "measurement_datetime"}),
                         ("s_obtained_datetime", SplitDateTimeWithTZ(), {"date": "measurement_date"}),
                         ("s_name", "measurement_source_value"),
                         ("s_code", measurement_code_mapper,  {"CONCEPT_ID".lower(): "measurement_source_concept_id"}),
                         ("s_code", measurement_code_mapper,  {"CONCEPT_ID".lower(): "measurement_concept_id"}),
                         ("s_code", measurement_type_chained_mapper, {"CONCEPT_ID".lower(): "measurement_type_concept_id"}),
                         (("s_result_numeric", "s_result_datetime"), NumericMapperConvertDate,
                          {"s_result_numeric": "value_as_number", "seconds_since_unix_epoch": "value_as_number"}),
                         (("s_result_code", "m_result_text"),
                          value_as_concept_mapper, {"CONCEPT_ID".lower(): "value_as_concept_id"}),
                         ("s_result_unit", "unit_source_value"),
                         (("s_result_unit_code", "s_result_unit"), unit_measurement_mapper, {"CONCEPT_ID".lower(): "unit_concept_id"}),
                         (("s_result_numeric", "m_result_text", "s_result_datetime", "s_result_code"),
                            value_source_mapper, # Map datetime to unix time
                          {"s_result_numeric": "value_source_value",
                           "m_result_text": "value_source_value",
                           "s_result_datetime": "value_source_value",
                          }),
                         ("s_result_numeric_lower", FloatMapper(), "range_low"),  # TODO: Some values contain non-numeric elements
                         ("s_result_numeric_upper", FloatMapper(), "range_high")]

    # TODO: observation_type_concept_id <- "Observation recorded from EHR"
    measurement_observation_rules = [(":row_id", "observation_id"),
                                     ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                                     ("s_encounter_id", s_encounter_id_mapper,
                                      {"visit_occurrence_id": "visit_occurrence_id"}),
                                     ("s_obtained_datetime", SplitDateTimeWithTZ(),
                                      {"date": "observation_date", "time": "observation_time"}),
                                     ("s_obtained_datetime", DateTimeWithTZ(), {"datetime": "observation_datetime"}),
                                     ("s_code", "observation_source_value"),
                                     ("s_code", measurement_code_mapper,
                                      {"CONCEPT_ID".lower(): "observation_source_concept_id"}),
                                     ("s_code", measurement_code_mapper,
                                      {"CONCEPT_ID".lower(): "observation_concept_id"}),
                                     ("s_code", measurement_type_chained_mapper,
                                      {"CONCEPT_ID".lower(): "observation_type_concept_id"}),
                                     (("s_result_numeric", "s_result_datetime"), NumericMapperConvertDate,
                                      {"s_result_numeric": "value_as_number",
                                       "seconds_since_unix_epoch": "value_as_number"}),
                                     (("s_result_code", "s_result_text"),
                                      value_as_concept_mapper, {"CONCEPT_ID".lower(): "value_as_concept_id"}),
                                     ("s_result_unit", "unit_source_value"),
                                     (("s_result_unit_code", "s_result_unit"), unit_measurement_mapper,
                                      {"CONCEPT_ID".lower(): "unit_concept_id"}),
                                     (("s_result_numeric", "s_result_text", "s_result_datetime"), value_source_mapper,
                                      {"s_result_numeric": "value_as_string",
                                       "s_result_text": "value_as_string",
                                       "s_result_datetime": "value_as_string"})]

    return measurement_rules, measurement_observation_rules


def drug_code_coding_system(input_dict, field="m_drug_code_oid"):
    """Determine from the OID the coding system for medication"""
    coding_system_oid = input_dict[field]

    if coding_system_oid == "2.16.840.1.113883.6.311":
        return "Multum Main Drug Code (MMDC)"
    elif coding_system_oid == "2.16.840.1.113883.6.312": # | MMSL - Multum drug synonym MMDC | BN - Fully specified drug brand name that can not be prescribed
        return "Multum drug synonym"
    elif coding_system_oid == "2.16.840.1.113883.6.314": # MMSL - GN - d04373 -- Generic drug name
        return "Multum drug identifier (dNUM)"
    elif coding_system_oid == "2.16.840.1.113883.6.88":
        return "RxNorm (RXCUI)"
    elif coding_system_oid == "2.16.840.1.113883.6.69":
        return "NDC"
    else:
        return False


def case_mapper_drug_with_full_multum_code(input_dict, field="m_drug_code_oid"):
    drug_coding_system_name = drug_code_coding_system(input_dict, field=field)

    if drug_coding_system_name == "Multum drug synonym":
        return 0
    elif drug_coding_system_name == "Multum drug identifier (dNUM)":
        return 1
    elif drug_coding_system_name == "Multum Main Drug Code (MMDC)":
        return 2
    elif drug_coding_system_name == "RxNorm (RXCUI)":
        return 3
    elif drug_coding_system_name == "NDC":
        return 4
    else:
        return False


def case_mapper_drug_code(input_dict, field="m_drug_code_oid"):
    drug_coding_system_name = drug_code_coding_system(input_dict, field=field)

    if drug_coding_system_name == "RxNorm (RXCUI)":
        return 0
    elif drug_coding_system_name == "NDC":
        return 1
    else:
        return False


def generate_rxcui_drug_code_mapper(json_map_directory):

    """Maps drug concepts to RxNorm CUIs"""

    multum_gn_json = os.path.join(json_map_directory, "RxNorm_MMSL_GN.json")
    multum_json = os.path.join(json_map_directory, "rxnorm_multum.csv.MULDRUG_ID.json")
    multum_drug_json = os.path.join(json_map_directory, "rxnorm_multum_drug.csv.MULDRUG_ID.json")
    multum_drug_mmdc_json = os.path.join(json_map_directory, "rxnorm_multum_mmdc.csv.MULDRUG_ID.json")

    ndc_code_mapper_json = os.path.join(json_map_directory, "NDC_with_parent.json")
    if os.path.exists(multum_json) and os.path.exists(multum_drug_json) and os.path.exists(multum_drug_mmdc_json):

        drug_code_mapper = ChainMapper(CaseMapper(case_mapper_drug_with_full_multum_code,
                                                  CodeMapperClassSqliteJSONClass(multum_json, "s_drug_code"),  # 0
                                                  CascadeMapper(
                                                    ChainMapper(CodeMapperClassSqliteJSONClass(multum_gn_json, "s_drug_code"),
                                                                KeyTranslator({"RXCUI": "RXNORM_ID"})),
                                                    CodeMapperClassSqliteJSONClass(multum_drug_json, "s_drug_code")),  # 1
                                                  CodeMapperClassSqliteJSONClass(multum_drug_mmdc_json, "s_drug_code"),  # 2
                                                  KeyTranslator({"s_drug_code": "RXNORM_ID"}),  # 3
                                                  CodeMapperClassSqliteJSONClass(ndc_code_mapper_json, "s_drug_code")  # 4
                                                  ))

    else:
        drug_code_mapper = ChainMapper(CaseMapper(case_mapper_drug_code,
                                                  KeyTranslator({"s_drug_code": "RXNORM_ID"}),  # 0
                                                  CodeMapperClassSqliteJSONClass(ndc_code_mapper_json, "s_drug_code")  # 1
                                                  ))

    return drug_code_mapper


def generate_drug_name_mapper(json_map_directory, drug_field_name="s_drug_text"):
    rxnorm_name_json = os.path.join(json_map_directory, "concept_name_RxNorm.json")
    rxnorm_name_mapper = CodeMapperClassSqliteJSONClass(rxnorm_name_json, drug_field_name)

    def string_to_cap_first_letters(raw_string):
        if len(raw_string):
            split_raw_string = raw_string.split(" ")
            capped_words = []
            for word in split_raw_string:
                if len(word):
                    capped_words += [word[0].upper() + word[1:].lower()]
            return " ".join(capped_words)
        else:
            return raw_string

    rxnorm_name_mapper_chained = CascadeMapper(rxnorm_name_mapper,
                                               ChainMapper(
                                                           TransformMapper(string_to_cap_first_letters), rxnorm_name_mapper))

    return rxnorm_name_mapper_chained


def generate_drug_name_alternative_mapper(json_map_directory):
    return generate_drug_name_mapper(json_map_directory, "s_drug_alternative_txt")


def create_medication_rules(json_map_directory, s_person_id_mapper, s_encounter_id_mapper, snomed_mapper, row_offset):

    # TODO: Increase mapping coverage of drugs - while likely need manual overrides

    rxnorm_rxcui_mapper = generate_rxcui_drug_code_mapper(json_map_directory)
    rxnorm_name_mapper_chained = CascadeMapper(generate_drug_name_mapper(json_map_directory),
                                               generate_drug_name_alternative_mapper(json_map_directory))

    # TODO: Increase coverage of "Map dose_unit_source_value -> drug_unit_concept_id"
    # TODO: Increase coverage of "Map route_source_value -> route_source_value"

    drug_type_json = os.path.join(json_map_directory, "concept_name_Drug_Type.json")
    drug_type_code_mapper = CoderMapperJSONClass(drug_type_json)

    rxnorm_code_mapper_json = os.path.join(json_map_directory, "concept_code_RxNorm.json")
    rxnorm_code_concept_mapper = CodeMapperClassSqliteJSONClass(rxnorm_code_mapper_json, "RXNORM_ID")
    drug_source_concept_mapper = CascadeMapper(ChainMapper(rxnorm_rxcui_mapper, rxnorm_code_concept_mapper),
                                                           rxnorm_rxcui_mapper,
                                                           rxnorm_name_mapper_chained)

    rxnorm_bn_in_mapper_json = os.path.join(json_map_directory,
                                             "select_n_in__ot___from___select_bn_rxcui.csv.bn_rxcui.json")
    rxnorm_bn_sbdf_mapper_json = os.path.join(json_map_directory,
                                              "select_tt_n_sbdf__ott___from___select_bn.csv.bn_rxcui.json")

    if os.path.exists(rxnorm_bn_in_mapper_json):
        rxnorm_bn_in_mapper = CodeMapperClassSqliteJSONClass(rxnorm_bn_in_mapper_json,"RXNORM_ID")
    else:
        rxnorm_bn_in_mapper = CodeMapperDictClass({})

    if os.path.exists(rxnorm_bn_sbdf_mapper_json):
        rxnorm_bn_sbdf_mapper = CodeMapperClassSqliteJSONClass(rxnorm_bn_sbdf_mapper_json, "RXNORM_ID")
    else:
        rxnorm_bn_sbdf_mapper = CodeMapperDictClass({})

    rxnorm_str_bn_in_mapper_json = os.path.join(json_map_directory,
                                                "select_n_in__ot___from___select_bn_rxcui.csv.bn_str.json")
    rxnorm_str_bn_sbdf_mapper_json = os.path.join(json_map_directory,
                                                  "select_tt_n_sbdf__ott___from___select_bn.csv.bn_str.json")

    if os.path.exists(rxnorm_str_bn_in_mapper_json):
        rxnorm_str_bn_in_mapper = CodeMapperClassSqliteJSONClass(rxnorm_str_bn_in_mapper_json)
    else:
        rxnorm_str_bn_in_mapper = CodeMapperDictClass({})

    if os.path.exists(rxnorm_str_bn_sbdf_mapper_json):
        rxnorm_str_bn_sbdf_mapper = CodeMapperClassSqliteJSONClass(rxnorm_str_bn_sbdf_mapper_json)
    else:
        rxnorm_str_bn_sbdf_mapper = CodeMapperDictClass({})

    rxnorm_concept_mapper = CascadeMapper(ChainMapper(CascadeMapper(
                                                                                ChainMapper(rxnorm_rxcui_mapper,
                                                                                ChainMapper(rxnorm_bn_sbdf_mapper,
                                                                                            KeyTranslator({"sbdf_rxcui": "RXNORM_ID"}))),
                                            ChainMapper(rxnorm_rxcui_mapper, ChainMapper(rxnorm_bn_in_mapper,
                                                                                       KeyTranslator({"in_rxcui": "RXNORM_ID"}))),
                                            rxnorm_rxcui_mapper), rxnorm_code_concept_mapper),

                                          CascadeMapper(ChainMapper(rxnorm_str_bn_sbdf_mapper, rxnorm_name_mapper_chained),
                                                        ChainMapper(rxnorm_str_bn_in_mapper, rxnorm_name_mapper_chained),
                                                        rxnorm_name_mapper_chained),

                                          ChainMapper(drug_source_concept_mapper,
                                                      KeyTranslator({"mapped_concept_id": "concept_id"}))
                                          )

    drug_type_mapper = ChainMapper(ReplacementMapper({"HOSPITAL_PHARMACY": "Inpatient administration",
                                                      "INPATIENT_FLOOR_STOCK": "Inpatient administration",
                                                      "RETAIL_PHARMACY": "Prescription dispensed in pharmacy",
                                                      "UNKNOWN": "Inpatient administration",
                                                      "_NOT_VALUED": "Prescription written",
                                                      "OFFICE": "Physician administered drug (identified from EHR observation)"
                                                      },
                                                     ),
                                   drug_type_code_mapper)

    # TODO: Rework this mapper not to be static code
    # Source: http://forums.ohdsi.org/t/route-standard-concepts-not-standard-anymore/1300/7
    routes_to_concept_id_dict = {
        "Gastroenteral": "4186834",
        "Cutaneous": "4263689",
        "Ocular": "4184451",
        "Intramuscular": "4302612",
        "Buccal": "4181897",
        "Nasal": "4262914",
        "Transdermal": "4262099",
        "Body cavity use": "4222254",
        "Vaginal": "4057765",
        "Intradermal": "4156706",
        "Epidural": "4225555",
        "Auricular": "4023156",
        "Intralesional": "4157758",
        "Intrauterine": "4269621",
        "Intraarticular": "4006860",
        "Intravesical": "4186838",
        "Dental": "4163765",
        "Intraperitoneal": "4243022",
        "Intravitreal": "4302785",
        "Intrapleural": "4156707",
        "Intrabursal": "4163768",
        "Intraosseous": "4213522",
        "Intravenous": "4112421",
        "Rectal": "4115462",
        "Inhaling": "4120036",
        "Oral": "4128794",
        "Subcutaneous": "4139962",
        "Intravaginal": "4136280",
        "Topical": "4231622",
        "Intraocular": "4157760",
        "Intrathecal": "4217202",
        "Urethral": "4233974"
    }

    route_mapper = ChainMapper(ReplacementMapper({"SubCutaneous": "Subcutaneous", "IV Push": "Intravenous",
                                                  "Continuous IV": "Intravenous", "IntraMuscular": "Intramuscular"}),
                               CodeMapperDictClass(routes_to_concept_id_dict))

    # Required # drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_type_concept_id
    medication_rules = [(":row_id", row_map_offset("drug_exposure_id", row_offset),
                                      {"drug_exposure_id": "drug_exposure_id"}),
                        ("s_person_id", s_person_id_mapper, {"person_id": "person_id"}),
                        ("s_encounter_id", s_encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                        (("s_drug_code", "s_drug_text"), ConcatenateMapper("|", "s_drug_code", "s_drug_text"),
                         {"s_drug_code|s_drug_text": "drug_source_value"}),
                        ("s_route", "route_source_value"),
                        ("s_status", "stop_reason"),
                        ("m_route", route_mapper, {"mapped_value": "route_concept_id"}),
                        ("s_dose", "dose_source_value"),
                        ("s_start_medication_datetime", SplitDateTimeWithTZ(), {"date": "drug_exposure_start_date"}),
                        ("s_end_medication_datetime", SplitDateTimeWithTZ(), {"date": "drug_exposure_end_date"}),
                        ("s_start_medication_datetime", DateTimeWithTZ(), {"datetime": "drug_exposure_start_datetime"}),
                        ("s_end_medication_datetime", DateTimeWithTZ(), {"datetime": "drug_exposure_end_datetime"}),
                        ("s_quantity", "quantity"),
                        ("s_dose_unit", ReplacementMapper({"NULL": ""}), "dose_unit_source_value"),
                        ("m_dose_unit", snomed_mapper, {"CONCEPT_ID".lower(): "dose_unit_concept_id"}),
                        (("m_drug_code_oid", "s_drug_code", "s_drug_text", "s_drug_alternative_text"), drug_source_concept_mapper,
                         {"CONCEPT_ID".lower(): "drug_source_concept_id"}),
                        (("m_drug_code_oid", "s_drug_code", "s_drug_text", "s_drug_alternative_text"), rxnorm_concept_mapper,
                         {"CONCEPT_ID".lower(): "drug_concept_id"}),  # TODO: Make sure map maps to standard concept
                        ("m_drug_type", drug_type_mapper, {"CONCEPT_ID".lower(): "drug_type_concept_id"})]

    return medication_rules


#### Routers #####

def person_router_obj(input_dict):
    """Route a person"""
    if input_dict["i_exclude"] == "1":
        return NoOutputClass()
    else:
        return PersonObject()


def death_router_obj(input_dict):
    """Determines if a row_dict codes a death"""
    if len(input_dict["s_death_datetime"]):
        if input_dict["s_death_datetime"] != "null":
            return DeathObject()
        else:
            return NoOutputClass()
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

