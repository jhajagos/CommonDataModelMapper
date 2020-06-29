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


class FloatMapper(MapperClass):
    """Convert value to float"""

    def map(self, input_dict):
        resulting_map = {}
        for key in input_dict:
            try:
                resulting_map[key] = float(input_dict[key])
            except(ValueError, TypeError):
                if input_dict[key] in ("NULL", "None", "null"):
                    resulting_map[key] = ""

        return resulting_map


class StringMapper(MapperClass):
    def map(self, input_dict):
        for key in input_dict:
            try:
                float_result = float(input_dict[key])
                return {key: ""}
            except(ValueError, TypeError):
                return {key: input_dict[key]}


# Define input classes
class SynPatient(InputClass):
    def fields(self):
        return ["Id", "BIRTHDATE", "DEATHDATE", "SSN", "DRIVERS", "PASSPORT", "PREFIX", "FIRST", "LAST", "SUFFIX",
                "MAIDEN", "MARITAL", "RACE", "ETHNICITY", "GENDER", "BIRTHPLACE", "ADDRESS", "CITY", "STATE", "COUNTY",
                "ZIP", "LAT", "LON", "HEALTHCARE_EXPENSES", "HEALTHCARE_COVERAGE"]


class SynEncounter(InputClass):
    def fields(self):
        return ["Id", "START", "STOP", "PATIENT", "PROVIDER", "PAYER", "ENCOUNTERCLASS", "CODE", "DESCRIPTION",
                "BASE_ENCOUNTER_COST", "TOTAL_CLAIM_COST", "PAYER_COVERAGE", "REASONCODE", "REASONDESCRIPTION"]


class SynMedication(InputClass):
    def fields(self):
        return ["START", "STOP", "PATIENT", "PAYER", "ENCOUNTER", "CODE", "DESCRIPTION", "BASE_COST", "PAYER_COVERAGE",
                "DISPENSES", "TOTALCOST", "REASONCODE", "REASONDESCRIPTION"]


class SynObservation(InputClass):
    """Notes: CODE is LOINC, "TYPE" will determine where value is placed"""
    def fields(self):
        return ["DATE", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION", "VALUE",	"UNITS", "TYPE"]


class SynCondition(InputClass):
    """Notes: CODE is SNOMED"""
    def fields(self):
        return ["START", "STOP", "PATIENT	ENCOUNTER", "CODE", "DESCRIPTION"]


class SynProcedure(InputClass):
    def fields(self):
        return ["DATE",	"PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION", "BASE_COST", "REASONCODE"]


class SynObservationPeriod(InputClass):
    def fields(self):
        return []


def strip_t_and_z(input_dict):
    result_dict = {}
    for key in input_dict:
        value = input_dict[key]
        new_string = ""
        for character in value:
            if character in ("T", "Z"):
                new_string += " "
            else:
                new_string += character

        new_string = new_string.strip()
        result_dict[key] = new_string

    return result_dict


def main(input_csv_directory, output_csv_directory, file_name_dict):

    oid_map = {
        "snomed": "2.16.840.1.113883.6.96",
        "loinc": "2.16.840.1.113883.6.1",
        "rxnorm": "2.16.840.1.113883.6.88"
    }


    clean_date_mapper = PassThroughFunctionMapper(strip_t_and_z)

    ### Patient

    input_patient_file_name = os.path.join(input_csv_directory, file_name_dict["patients"])

    race_map = {
        "asian": "Asian",
        "black": "Black",
        "native": "Native",
        "other": "Other",
        "white": "White"
    }
    race_mapper = CodeMapperDictClass(race_map)

    ethnicity_map = {
        "hispanic": "Hispanic"
    }
    ethnicity_mapper = CodeMapperDictClass(ethnicity_map)

    gender_map = {
        "F": "Female",
        "M": "Male"
    }
    gender_mapper = CodeMapperDictClass(gender_map)

    output_class_obj = OutputClassDirectory()
    in_out_map_obj = InputOutputMapperDirectory()

    syn_patient_rules = [("Id", "s_person_id"),
                         ("GENDER", "s_gender"),
                         ("GENDER", gender_mapper, {"mapped_value": "m_gender"}),
                         ("BIRTHDATE", "s_birth_datetime"),
                         ("RACE", "s_race"),
                         ("RACE", race_mapper, {"mapped_value": "m_race"}),
                         ("ETHNICITY", "s_ethnicity"),
                         ("ETHNICITY", ethnicity_mapper, {"mapped_value": "m_ethnicity"})]

    output_person_csv = os.path.join(output_csv_directory, "source_person.csv")

    source_person_runner_obj = generate_mapper_obj(input_patient_file_name, SynPatient(), output_person_csv,
                                                   SourcePersonObject(), syn_patient_rules,
                                                   output_class_obj, in_out_map_obj)

    source_person_runner_obj.run()  # Run the mapper

    ### Encounters

    encounter_file_name = os.path.join(input_csv_directory, file_name_dict["encounters"])

    encounter_type_map = {
        "ambulatory": "Outpatient",
        "emergency": "Emergency",
        "inpatient": "Inpatient",
        "outpatient": "Outpatient",
        "urgentcare": "Outpatient",
        "wellness": "Outpatient"
    }

    encounter_type_mapper = CodeMapperDictClass(encounter_type_map)

    encounter_rules = [
        ("Id", "s_encounter_id"),
        ("PATIENT", "s_person_id"),
        ("START", clean_date_mapper, {"START": "s_visit_start_datetime"}),
        ("STOP", clean_date_mapper, {"STOP": "s_visit_end_datetime"}),
        ("ENCOUNTERCLASS", "s_visit_type"),
        ("ENCOUNTERCLASS", encounter_type_mapper, {"mapped_value": "m_visit_type"})
    ]

    source_encounter_csv = os.path.join(output_csv_directory, "source_encounter.csv")

    encounter_runner_obj = generate_mapper_obj(encounter_file_name, SynEncounter(), source_encounter_csv,
                                               SourceEncounterObject(), encounter_rules,
                                               output_class_obj, in_out_map_obj)

    encounter_runner_obj.run()

    observation_csv_file = os.path.join(input_csv_directory, "syn_observation.csv")

    generate_observation_period(source_encounter_csv, observation_csv_file,
                                "s_person_id", "s_visit_start_datetime", "s_visit_end_datetime")

    observation_period_rules = [("s_person_id", "s_person_id"),
                                ("s_visit_start_datetime", "s_start_observation_datetime"),
                                ("s_visit_end_datetime", "s_end_observation_datetime")]

    source_observation_period_csv = os.path.join(output_csv_directory, "source_observation_period.csv")

    observation_runner_obj = generate_mapper_obj(observation_csv_file, SynObservationPeriod(),
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

    ### Conditions

    condition_rules = [("PATIENT", "s_person_id"),
                       ("ENCOUNTER", "s_encounter_id"),
                       ("CODE", "s_condition_code"),
                       ("PATIENT", ConstantMapper({"type": "snomed"}), {"type": "s_condition_code_type"}),
                       ("PATIENT", ConstantMapper({"oid": oid_map["snomed"]}), {"oid": "m_condition_code_oid"}),
                       ("PATIENT", ConstantMapper({"type": "Not Mapped"}), {"type": "s_condition_type"}),
                       ("START", "s_start_condition_datetime"),
                       ("STOP", "s_end_condition_datetime")
                       ]

    syn_diagnosis_csv = os.path.join(input_csv_directory, file_name_dict["diagnoses"])
    source_condition_csv = os.path.join(output_csv_directory, "source_condition.csv")
    condition_mapper_obj = generate_mapper_obj(syn_diagnosis_csv, SynEncounter(), source_condition_csv,
                                               SourceConditionObject(),
                                               condition_rules, output_class_obj, in_out_map_obj)

    condition_mapper_obj.run()

    ### Procedures

    syn_procedure_csv = os.path.join(input_csv_directory, file_name_dict["procedures"])
    source_procedure_csv = os.path.join(output_csv_directory, "source_procedure.csv")

    procedure_rules = [("PATIENT", "s_person_id"),
                       ("ENCOUNTER", "s_encounter_id"),
                       ("DATE", "s_start_procedure_datetime"),
                       ("CODE", "s_procedure_code"),
                       ("PATIENT", ConstantMapper({"type": "snomed"}), {"type": "s_procedure_code_type"}),
                       ("PATIENT", ConstantMapper({"oid": oid_map["snomed"]}), {"oid": "m_procedure_code_oid"})
                      ]

    procedure_mapper_obj = generate_mapper_obj(syn_procedure_csv, SynProcedure(), source_procedure_csv,
                                               SourceProcedureObject(),
                                               procedure_rules, output_class_obj, in_out_map_obj)

    procedure_mapper_obj.run()

    ### Observations
    result_rules = [
        ("PATIENT", "s_person_id"),
        ("ENCOUNTER", "s_encounter_id"),
        ("DATE", "s_obtained_datetime"),
        ("DESCRIPTION", "s_name"),
        ("CODE", "s_code"),
        ("PATIENT", ConstantMapper({"oid": oid_map["loinc"]}), {"oid": "m_type_code_oid"}),
        ("PATIENT", ConstantMapper({"type": "loinc"}), {"type": "s_type_code"}),
        ("VALUE", "s_result_text"),
        ("VALUE", StringMapper(),  {"VALUE": "m_result_text"}),
        ("VALUE", FloatMapper(), {"VALUE": "s_result_numeric"}),
        ("UNITS", "s_result_unit"),
        ("UNITS", "m_result_unit")
    ]

    syn_observation_csv = os.path.join(input_csv_directory, file_name_dict["observations"])
    source_result_csv = os.path.join(output_csv_directory, "source_result.csv")

    result_mapper_obj = generate_mapper_obj(syn_observation_csv, SynObservation(), source_result_csv, SourceResultObject(),
                                            result_rules, output_class_obj, in_out_map_obj)

    result_mapper_obj.run()

    ### Medication
    medication_rules = [
        ("PATIENT", "s_person_id"),
        ("ENCOUNTER", "s_encounter_id"),
        ("CODE", "s_drug_code"),
        (":row_id", ConstantMapper({"oid": oid_map["rxnorm"]}), {"oid": "m_drug_code_oid"}),
        (":row_id", ConstantMapper({"s_drug_code_type": "rxnorm"}), {"s_drug_code_type": "s_drug_code_type"}),
        ("DESCRIPTION", "s_drug_text"),
        ("START", "s_start_medication_datetime"),
        ("STOP", "s_end_medication_datetime"),
        ("DISPENSES", "s_quantity"),
        (":row_id", ConstantMapper({"drug_type": "UNKNOWN"}), {"drug_type": "m_drug_type"})
    ]

    syn_medication_csv = os.path.join(input_csv_directory, file_name_dict["medications"])
    source_medication_csv = os.path.join(output_csv_directory, "source_medication.csv")

    medication_mapper_obj = generate_mapper_obj(syn_medication_csv, SynMedication(), source_medication_csv,
                                                SourceMedicationObject(), medication_rules,
                                                output_class_obj, in_out_map_obj)

    medication_mapper_obj.run()


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Mapping SYNTHEA CSV files to Prepared source format for OHDSI mapping")
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="syn_config.json")
    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as f:
        config_dict = json.load(f)

    file_name_dict = {
        "patients": "patients.csv",
        "diagnoses": "conditions.csv",
        "encounters": "encounters.csv",
        "observations": "observations.csv",
        "medications": "medications.csv",
        "procedures": "procedures.csv"
    }

    main(config_dict["csv_input_directory"], config_dict["csv_input_directory"], file_name_dict)

