import logging
import json
import os
import argparse
import csv
import hashlib
import sys

try:
    from mapping_classes import InputClass
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, "src")))

from mapping_classes import InputClass

from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
            CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper, ChainMapper, CascadeKeyMapper, \
            CascadeMapper, KeyTranslator, PassThroughFunctionMapper, CodeMapperDictClass, CodeMapperDictClass, ConstantMapper, \
            ReplacementMapper, MapperClass

from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
        SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
        SourceProcedureObject, SourceMedicationObject, SourceLocationObject, SourceEncounterDetailObject

from source_to_cdm_functions import generate_mapper_obj
from utility_functions import generate_observation_period

from prepared_source_functions import build_name_lookup_csv, build_key_func_dict

logging.basicConfig(level=logging.INFO)


class PopulationDemographics(InputClass):
    def fields(self):
        return ["empipersonid", "gender_code", "gender_code_oid", "gender_code_text", "birthsex_code", "birthsex_code_oid",
                "birthsex_code_text", "birthdate", "dateofdeath", "zip_code", "race_code", "race_code_oid", "race_code_text",
                "ethnicity_code", "ethnicity_code_oid", "ethnicity_code_text", "active", "tenant"]


class PopulationEncounter(InputClass):
    def fields(self):
        return ["encounterid", "empipersonid", "hospitalizationstartdate", "readmission", "dischargedate", "servicedate",
                "financialclass_code", "financialclass_code_oid", "financialclass_code_text", "hospitalservice_code",
                "hospitalservice_code_oid", "hospitalservice_code_text", "classfication_code", "classification_code_oid",
                "classification_code_text", "type_code", "type_code_oid", "type_code_text", "dischargedisposition_code",
                "dischargedisposition_code_oid", "dischargedisposition_code_text", "dischargetolocation_code",
                "dischargetolocation_code_oid", "dischargetolocation_code_text", "admissionsource_code",
                "admissionsource_code_oid", "admissionsource_code_text", "admissiontype_code", "admissiontype_code_oid",
                "admissiontype_code_text", "status_code", "status_code_oid", "status_code_text", "estimatedarrivaldate",
                "estimateddeparturedate", "actualarrivaldate", "source", "active", "tenant"]


class PopulationCondition(InputClass):
    def fields(self):
        return ["conditionid", "empipersonid", "encounterid", "condition_code", "condition_code_oid", "condition_code_text",
                "effectiveDate", "billingrank", "presentonadmission_code", "presentonadmission_code_oid",
                "presentonadmission_text", "type_primary_code", "type_primary_code_oid", "type_primary_text",
                "source", "tenant"]


class PopulationProcedure(InputClass):
    def fields(self):
        return ["procedureid", "empipersonid", "encounterid", "procedure_code", "procedure_code_oid",
                "procedure_code_display", "modifier_code", "modifier_oid", "modifier_text", "servicestartdate",
                "serviceenddate", "status_code", "status_oid", "active", "tenant"]


class PopulationMedication(InputClass):
    def fields(self):
        return ["medicationid", "encounterid", "empipersonid", "intendeddispenser", "startdate", "stopdate", "doseunit_code",
                "doseunit_code_oid", "doseunit_code_text", "category_id", "category_code_oid", "category_code_text",
                "frequency_id", "frequency_code_oid", "frequency_code_text", "status_code", "status_code_oid",
                "status_code_text", "route_code", "route_code_oid", "route_code_text", "drug_code", "drug_code_oid",
                "drug_code_text", "dosequantity", "source", "tenant"]


class PopulationResult(InputClass):
    def fields(self):
        return ["resultid", "encounterid", "empipersonid", "result_code", "result_code_oid", "result_code_text",
                "result_type", "servicedate", "value_text", "value_numeric", "value_numeric_modifier", "unit_code",
                "unit_code_oid", "unit_code_text", "value_codified_code", "value_codified_code_oid",
                "value_codified_code_text", "date", "interpretation_code", "interpretation_code_oid",
                "interpretation_code_text", "specimen_type_code", "specimen_type_code_oid", "specimen_type_code_text",
                "bodysite_code", "bodysite_code_oid", "bodysite_code_text", "specimen_collection_date",
                "specimen_received_date", "measurementmethod_code", "measurementmethod_code_oid",
                "measurementmethod_code_text", "recordertype", "issueddate", "tenant", "year"]


class PopulationObservationPeriod(InputClass):
    def fields(self):
        return []


class PopulationCareSite(InputClass):
    def fields(self):
        return []


class DuplicateExcludeMapper(MapperClass):
    """Indicates that a row is a duplicate"""
    def __init__(self, id_field):
        self.id_field = id_field
        self.id_dict = {"i_exclude": ""}

    def map(self, input_dict):
        if self.id_field in input_dict:
            id_value = input_dict[self.id_field]

            if id_value in self.id_dict:
                return {"i_exclude": 1}

            else:
                self.id_dict[id_value] = 1
                return {"i_exclude": ""}

        else:
            return {}


def main(input_csv_directory, output_csv_directory, file_name_dict):

    output_class_obj = OutputClassDirectory()
    in_out_map_obj = InputOutputMapperDirectory()

    # TOOD: Add single digit zip code
    sec_fields = SourceLocationObject().fields()
    with open(os.path.join(output_csv_directory, "source_location.csv"), newline="", mode="w") as fw:
        cfw = csv.writer(fw)
        cfw.writerow(sec_fields)

    input_patient_file_name = os.path.join(input_csv_directory, file_name_dict["demographic"])

    person_id_duplicate_mapper = DuplicateExcludeMapper("empipersonid")
    population_patient_rules = [("empipersonid", "s_person_id"),
                                ("gender_code_text", "s_gender"),
                                ("gender_code",  "m_gender"),
                                ("birthdate", "s_birth_datetime"),
                                ("dateofdeath", "s_death_datetime"),
                                ("race_code", "s_race"),
                                ("race_code_text",  "m_race"),
                                ("ethnicity_code", "s_ethnicity"),
                                ("ethnicity_code_text", "m_ethnicity"),
                                ("empipersonid", person_id_duplicate_mapper, {"i_exclude": "i_exclude"})
                                ]

    output_person_csv = os.path.join(output_csv_directory, "source_person.csv")

    source_person_runner_obj = generate_mapper_obj(input_patient_file_name, PopulationDemographics(), output_person_csv,
                                                   SourcePersonObject(), population_patient_rules,
                                                   output_class_obj, in_out_map_obj)

    source_person_runner_obj.run()  # Run the mapper

    # Care site
    care_site_csv = os.path.join(input_csv_directory, "care_site.csv")
    md5_func = lambda x: hashlib.md5(x.encode("utf8")).hexdigest()

    key_care_site_mapper = build_name_lookup_csv(os.path.join(input_csv_directory, file_name_dict["encounter"]), care_site_csv,
                                                 ["tenant", "hospitalservice_code_text"],
                                                 ["tenant", "hospitalservice_code_text"], hashing_func=md5_func)

    care_site_name_mapper = FunctionMapper(
        build_key_func_dict(["tenant", "hospitalservice_code_text"], separator=" -- "))

    care_site_rules = [("key_name", "k_care_site"),
                       (("tenant", "hospitalservice_code_text"),
                        care_site_name_mapper,
                        {"mapped_value": "s_care_site_name"})]

    source_care_site_csv = os.path.join(output_csv_directory, "source_care_site.csv")

    care_site_runner_obj = generate_mapper_obj(care_site_csv, PopulationCareSite(), source_care_site_csv,
                                               SourceCareSiteObject(), care_site_rules,
                                               output_class_obj, in_out_map_obj)

    care_site_runner_obj.run()


    # Encounters
    # TODO: Add flag for duplicate encounters
    encounter_file_name = os.path.join(input_csv_directory, file_name_dict["encounter"])
    encounter_id_duplicate_mapper = DuplicateExcludeMapper("encounterid")
    encounter_rules = [
        ("encounterid", "s_encounter_id"),
        ("empipersonid", "s_person_id"),
        ("servicedate", "s_visit_start_datetime"),
        ("dischargedate", "s_visit_end_datetime"),
        ("type_code_text", "s_visit_type"),
        ("classification_code_text", "m_visit_type"),
        ("dischargedisposition_code_text", "s_discharge_to"),
        ("dischargedisposition_code", "m_discharge_to"),
        ("admissionsource_code_text", "s_admitting_source"),
        ("admissionsource_code", "m_admitting_source"),
        (("tenant", "hospitalservice_code_text"), key_care_site_mapper, {"mapped_value": "k_care_site"}),
        ("encounterid", encounter_id_duplicate_mapper, {"i_exclude": "i_exclude"})
    ]

    source_encounter_csv = os.path.join(output_csv_directory, "source_encounter.csv")

    # Generate care site combination of tenant and hospitalservice_code_text

    encounter_runner_obj = generate_mapper_obj(encounter_file_name, PopulationEncounter(), source_encounter_csv,
                                               SourceEncounterObject(), encounter_rules,
                                               output_class_obj, in_out_map_obj)

    encounter_runner_obj.run()

    observation_csv_file = os.path.join(input_csv_directory, "population_observation.csv")

    generate_observation_period(source_encounter_csv, observation_csv_file,
                                "s_person_id", "s_visit_start_datetime", "s_visit_end_datetime")

    observation_period_rules = [("s_person_id", "s_person_id"),
                                ("s_visit_start_datetime", "s_start_observation_datetime"),
                                ("s_visit_end_datetime", "s_end_observation_datetime")]

    source_observation_period_csv = os.path.join(output_csv_directory, "source_observation_period.csv")

    observation_runner_obj = generate_mapper_obj(observation_csv_file, PopulationObservationPeriod(),
                                                 source_observation_period_csv,
                                                 SourceObservationPeriodObject(), observation_period_rules,
                                                 output_class_obj, in_out_map_obj)
    observation_runner_obj.run()

    # Holder for source encounter coverage
    sec_fields = SourceEncounterDetailObject().fields()
    with open(os.path.join(output_csv_directory, "source_encounter_detail.csv"), newline="", mode="w") as fw:
        cfw = csv.writer(fw)
        cfw.writerow(sec_fields)

    # Encounter plan or insurance coverage

    source_encounter_coverage_csv = os.path.join(output_csv_directory, "source_encounter_coverage.csv")

    encounter_coverage_rules = [("empipersonid", "s_person_id"),
                                ("encounterid", "s_encounter_id"),
                                ("servicedate", "s_start_payer_date"),
                                ("dischargedate", "s_end_payer_date"),
                                ("financialclass_code_text", "s_payer_name"),
                                ("financialclass_code_text", "m_payer_name"),
                                ("financialclass_code_text", "s_plan_name"),
                                ("financialclass_code_text", "m_plan_name")]

    encounter_benefit_runner_obj = generate_mapper_obj(encounter_file_name,
                                                       PopulationEncounter(),
                                                       source_encounter_coverage_csv, SourceEncounterCoverageObject(),
                                                       encounter_coverage_rules, output_class_obj, in_out_map_obj)

    encounter_benefit_runner_obj.run()

    def m_rank_func(input_dict):
        if input_dict["billingrank"] == "PRIMARY":
            return {"m_rank": "Primary"}
        elif input_dict["billingrank"] == "SECONDARY":
            return {"m_rank": "Secondary"}
        else:
            return {}

    condition_rules = [("empipersonid", "s_person_id"),
                       ("encounterid", "s_encounter_id"),
                       ("effectiveDate", "s_start_condition_datetime"),
                       ("condition_code", "s_condition_code"),
                       ("condition_code_oid", "m_condition_code_oid"),
                       ("billingrank", PassThroughFunctionMapper(m_rank_func), {"m_rank": "m_rank"}),
                       ("source", "s_condition_type"),
                       ("presentonadmission_code", "s_present_on_admission_indicator")]

    condition_csv = os.path.join(input_csv_directory, file_name_dict["condition"])
    source_condition_csv = os.path.join(output_csv_directory, "source_condition.csv")
    condition_mapper_obj = generate_mapper_obj(condition_csv, PopulationCondition(), source_condition_csv,
                                               SourceConditionObject(),
                                               condition_rules, output_class_obj, in_out_map_obj)

    condition_mapper_obj.run()

    procedure_csv = os.path.join(input_csv_directory, file_name_dict["procedure"])
    source_procedure_csv = os.path.join(output_csv_directory, "source_procedure.csv")

    procedure_rules = [("empipersonid", "s_person_id"),
                       ("encounterid", "s_encounter_id"),
                       ("servicestartdate", "s_start_procedure_datetime"),
                       ("serviceenddate", "s_end_procedure_datetime"),
                       ("procedure_code", "s_procedure_code"),
                       ("procedure_code_oid", "s_procedure_code_type"),
                       ("procedure_code_oid", "m_procedure_code_oid")
                       ]

    procedure_mapper_obj = generate_mapper_obj(procedure_csv, PopulationProcedure(), source_procedure_csv,
                                               SourceProcedureObject(),
                                               procedure_rules, output_class_obj, in_out_map_obj)

    procedure_mapper_obj.run()

    def active_medications(input_dict):
        if "status_code_text" in input_dict:
            if input_dict["status_code_text"] not in ('Complete', 'Discontinued', 'Active', 'Suspended'):
                return {"i_exclude": 1}
            else:
                return {}
        else:
            return {}

    ["medicationid", "encounterid", "empipersonid", "intendeddispenser", "startdate", "stopdate", "doseunit_code",
     "doseunit_code_oid", "doseunit_code_text", "category_id", "category_code_oid", "category_code_text",
     "frequency_id", "frequency_code_oid", "frequency_code_text", "status_code", "status_code_oid",
     "status_code_text", "route_code", "route_code_oid", "route_code_text", "drug_code", "drug_code_oid",
     "drug_code_text", "dosequantity", "source", "tenant"]

    medication_rules = [("empipersonid", "s_person_id"),
                        ("encounterid", "s_encounter_id"),
                        ("drug_code", "s_drug_code"),
                        ("drug_code_oid", "m_drug_code_oid"),
                        ("drug_code_text", "s_drug_text"),
                        ("startdate", "s_start_medication_datetime"),
                        ("stopdate", "s_end_medication_datetime"),
                        ("route_code_text", "s_route"),
                        ("route_code", "m_route"),
                        ("dosequantity", "s_quantity"),
                        ("doseunit_code_text", "s_dose_unit"),
                        ("doseunit_code", "m_dose_unit"),
                        ("intendeddispenser", "s_drug_type"),
                        ("intendeddispenser", "m_drug_type"),
                        ("status_code", "s_status"),
                        ("status_code_text", PassThroughFunctionMapper(active_medications),
                         {"i_exclude": "i_exclude"})
                        ]

    medication_csv = os.path.join(input_csv_directory, file_name_dict["medication"])
    source_medication_csv = os.path.join(output_csv_directory, "source_medication.csv")

    medication_mapper_obj = generate_mapper_obj(medication_csv, PopulationMedication(), source_medication_csv,
                                                SourceMedicationObject(), medication_rules,
                                                output_class_obj, in_out_map_obj)

    medication_mapper_obj.run()

    result_csv = os.path.join(input_csv_directory, file_name_dict["result"])
    source_result_csv = os.path.join(output_csv_directory, "source_result.csv")

    ["resultid", "encounterid", "empipersonid", "result_code", "result_code_oid", "result_code_text",
     "result_type", "servicedate", "value_text", "value_numeric", "value_numeric_modifier", "unit_code",
     "unit_code_oid", "unit_code_text", "value_codified_code", "value_codified_code_oid",
     "value_codified_code_text", "date", "interpretation_code", "interpretation_code_oid",
     "interpretation_code_text", "specimen_type_code", "specimen_type_code_oid", "specimen_type_code_text",
     "bodysite_code", "bodysite_code_oid", "bodysite_code_text", "specimen_collection_date",
     "specimen_received_date", "measurementmethod_code", "measurementmethod_code_oid",
     "measurementmethod_code_text", "recordertype", "issueddate", "tenant", "year"]

    result_rules = [("empipersonid", "s_person_id"),
                    ("encounterid", "s_encounter_id"),
                    ("servicedate", "s_obtained_datetime"),
                    ("result_code_text", "s_name"),
                    ("result_code", "s_code"),
                    ("result_code_oid", "m_type_code_oid"),
                    ("value_text", "s_result_text"),
                    (("value_codified_code_text", "interpretation_code_text"),
                     FilterHasKeyValueMapper(["value_codified_code_text", "interpretation_code_text"]),
                     {"value_codified_code_text": "m_result_text", "interpretation_code_text": "m_result_text"}),
                    ("value_numeric", "s_result_numeric"),
                    ("date", "s_result_datetime"),
                    ("value_codified_code", "s_result_code"),
                    ("value_codified_code_oid", "m_result_code_oid"),
                    ("unit_code", "s_result_unit"),
                    ("unit_code", "s_result_unit_code"),
                    ("unit_code_oid", "m_result_unit_code_oid")
                    #("norm_unit_of_measure_code", "s_result_unit_code")
                    #("norm_ref_range_low", "s_result_numeric_lower"),
                    #("norm_ref_range_high", "s_result_numeric_upper")
                    ]

    result_mapper_obj = generate_mapper_obj(result_csv, PopulationResult(), source_result_csv, SourceResultObject(),
                                            result_rules, output_class_obj, in_out_map_obj)

    result_mapper_obj.run()


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser(description="Mapping Realworld CSV files to Prepared source format for OHDSI mapping")
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="rw_config.json")

    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as f:
        config_dict = json.load(f)

    file_name_dict = {
        "demographic": "population_demographics.csv",
        "encounter": "population_encounter.csv",
        "condition": "population_condition.csv",
        "measurement": "population_measurement.csv",
        "medication": "population_medication.csv",
        "procedure": "population_procedure.csv",
        "result": "population_results_2020.csv"
    }

    main(config_dict["csv_input_directory"], config_dict["csv_input_directory"], file_name_dict)