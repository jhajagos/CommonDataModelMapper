import logging
import json
import os
import argparse
import csv
from mapping_classes import InputClass

from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
            CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper, ChainMapper, CascadeKeyMapper, \
            CascadeMapper, KeyTranslator, PassThroughFunctionMapper, CodeMapperDictClass, CodeMapperDictClass, ConstantMapper, \
            ReplacementMapper, MapperClass

from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
        SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
        SourceProcedureObject, SourceMedicationObject

from source_to_cdm_functions import generate_mapper_obj
from utility_functions import generate_observation_period


logging.basicConfig(level=logging.INFO)


class PopulationDemographics(InputClass):
    def fields(self):
        return ["personid", "gender_code", "gender_code_oid", "gender_code_text", "birthsex_code", "birthsex_code_oid",
                "birthsex_code_text", "birthdate", "dateofdeath", "zip_code", "race_code", "race_code_oid", "race_code_text",
                "ethnicity_code", "ethnicity_code_oid", "ethnicity_code_text", "active", "tenant"]


class PopulationEncounter(InputClass):
    def fields(self):
        return ["encounterid", "personid", "hospitalizationstartdate", "readmission", "dischargedate", "servicedate",
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
        return ["conditionid", "personid", "encounterid", "condition_code", "condition_code_oid", "condition_code_text",
                "effectiveDate", "billingrank", "presentonadmission_code", "presentonadmission_code_oid",
                "presentonadmission_text", "type_primary_code", "type_primary_code_oid", "type_primary_text",
                "source", "tenant"]


class PopulationProcedure(InputClass):
    def fields(self):
        return ["procedureid", "personid", "encounterid", "procedure_code", "procedure_code_oid",
                "procedure_code_display", "modifier_code", "modifier_oid", "modifier_text", "servicestartdate",
                "serviceenddate", "status_code", "status_oid", "active", "tenant"]


class PopulationMedication(InputClass):
    def fields(self):
        return ["medicationid", "encounterid", "personid", "intendeddispenser", "startdate", "stopdate", "doseunit_code",
                "doseunit_code_oid", "doseunit_code_text", "category_id", "category_code_oid", "category_code_text",
                "frequency_id", "frequency_code_oid", "frequency_code_text", "status_code", "status_code_oid",
                "status_code_text", "route_code", "route_code_oid", "route_code_text", "drug_code", "drug_code_oid",
                "drug_code_text", "dosequantity", "source", "tenant"]


class PopulationResult(InputClass):
    def fields(self):
        return ["resultid", "encounterid", "personid", "result_code", "result_code_oid", "result_code_text",
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


def main(input_csv_directory, output_csv_directory, file_name_dict):

    output_class_obj = OutputClassDirectory()
    in_out_map_obj = InputOutputMapperDirectory()

    input_patient_file_name = os.path.join(input_csv_directory, file_name_dict["demographic"])

    syn_patient_rules = [("personid", "s_person_id"),
                         ("gender_code_text", "s_gender"),
                         ("gender_code",  "m_gender"),
                         ("birthdate", "s_birth_datetime"),
                         ("dateofdeath", "s_death_datetime"),
                         ("race_code_text", "s_race"),
                         ("race_code",  "m_race"),
                         ("ethnicity_code_text", "s_ethnicity"),
                         ("ethnicity_code", "m_ethnicity")]

    output_person_csv = os.path.join(output_csv_directory, "source_person.csv")

    source_person_runner_obj = generate_mapper_obj(input_patient_file_name, PopulationDemographics(), output_person_csv,
                                                   SourcePersonObject(), syn_patient_rules,
                                                   output_class_obj, in_out_map_obj)

    source_person_runner_obj.run()  # Run the mapper

    ### Encounters

    encounter_file_name = os.path.join(input_csv_directory, file_name_dict["encounter"])

    # encounter_type_map = {
    #     "ambulatory": "Outpatient",
    #     "emergency": "Emergency",
    #     "inpatient": "Inpatient",
    #     "outpatient": "Outpatient",
    #     "urgentcare": "Outpatient",
    #     "wellness": "Outpatient"
    # }

    # encounter_type_mapper = CodeMapperDictClass(encounter_type_map)

    # return ["encounterid", "personid", "hospitalizationstartdate", "readmission", "dischargedate", "servicedate",
    #         "financialclass_code", "financialclass_code_oid", "financialclass_code_text", "hospitalservice_code",
    #         "hospitalservice_code_oid", "hospitalservice_code_text", "classfication_code", "classification_code_oid",
    #         "classification_code_text", "type_code", "type_code_oid", "type_code_text", "dischargedisposition_code",
    #         "dischargedisposition_code_oid", "dischargedisposition_code_text", "dischargetolocation_code",
    #         "dischargetolocation_code_oid", "dischargetolocation_code_text", "admissionsource_code",
    #         "admissionsource_code_oid", "admissionsource_code_text", "admissiontype_code", "admissiontype_code_oid",
    #         "admissiontype_code_text", "status_code", "status_code_oid", "status_code_text", "estimatedarrivaldate",
    #         "estimateddeparturedate", "actualarrivaldate", "source", "active", "tenant"]

    encounter_rules = [
        ("encounterid", "s_encounter_id"),
        ("personid", "s_person_id"),
        ("servicedate", "s_visit_start_datetime"),
        ("dischargedate", "s_visit_end_datetime"),
        ("type_code_text", "s_visit_type"),
        ("classification_code_text", "m_visit_type"),
        ("dischargedisposition_code_text", "s_discharge_to"),
        ("dischargedisposition_code", "m_discharge_to"),
        ("admissionsource_code_text", "s_admitting_source"),
        ("admissionsource_code", "m_admitting_source")
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

    ### Holder for source_care_site
    sc_fields = SourceCareSiteObject().fields()
    with open(os.path.join(output_csv_directory, "source_care_site.csv"), newline="", mode="w") as fw:
        cfw = csv.writer(fw)
        cfw.writerow(sc_fields)

    ### Holder for source encounter coverage
    sec_fields = SourceEncounterCoverageObject().fields()
    with open(os.path.join(output_csv_directory, "source_encounter_coverage.csv"), newline="", mode="w") as fw:
        cfw = csv.writer(fw)
        cfw.writerow(sec_fields)



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
        "medications": "population_medications.csv",
        "procedures": "population_procedures.csv"
    }

    main(config_dict["csv_input_directory"], config_dict["csv_input_directory"], file_name_dict)