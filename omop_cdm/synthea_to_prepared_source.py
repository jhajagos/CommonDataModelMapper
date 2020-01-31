import logging
import json
import os
import argparse
import csv
from mapping_classes import InputClass
from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
            CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper, ChainMapper, CascadeKeyMapper, \
            CascadeMapper, KeyTranslator, PassThroughFunctionMapper, CodeMapperDictClass, CodeMapperDictClass, ConstantMapper, \
            ReplacementMapper

from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
        SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
        SourceProcedureObject, SourceMedicationObject

from source_to_cdm_functions import generate_mapper_obj

logging.basicConfig(level=logging.INFO)


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


def generate_observation_period(encounter_csv_file_name, syn_period_observation_csv_file_name,
                                id_field_name, start_date_field_name, end_date_field_name):

    with open(encounter_csv_file_name, newline="") as f:
        dict_reader = csv.DictReader(f)
        observation_period_dict = {}

        for row_dict in dict_reader:

            start_date_value = row_dict[start_date_field_name]
            end_date_value = row_dict[end_date_field_name]

            if len(end_date_value) == 0:
                end_date_value = start_date_value

            id_value = row_dict[id_field_name]

            if id_value in observation_period_dict:
                past_start_date_value, past_end_date_value = observation_period_dict[id_value]

                if start_date_value < past_start_date_value:
                    set_start_date_value = start_date_value
                else:
                    set_start_date_value = past_start_date_value

                if end_date_value > past_end_date_value:
                    set_end_date_value = end_date_value
                else:
                    set_end_date_value = past_end_date_value

                observation_period_dict[id_value] = (set_start_date_value, set_end_date_value)

            else:
                observation_period_dict[id_value] = (start_date_value, end_date_value)

    with open(syn_period_observation_csv_file_name, "w", newline="") as fw:
        csv_writer = csv.writer(fw)

        csv_writer.writerow([id_field_name, start_date_field_name, end_date_field_name])

        for id_value in observation_period_dict:
            start_date_value, end_date_value = observation_period_dict[id_value]
            if start_date_value == "":
                start_date_value = end_date_value
            row_to_write = [id_value, start_date_value, end_date_value]
            csv_writer.writerow(row_to_write)


def main(input_csv_directory, output_csv_directory, file_name_dict):

    oid_map = {
        "snomed": "2.16.840.1.113883.6.96",
        "loinc": "2.16.840.1.113883.6.1",
        "rxnorm": "2.16.840.1.113883.6.88"
    }

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
                         ("ETHNICITY", ethnicity_mapper, {"mapped_value": "m_ethnicity"})
                        ]

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
        ("START", "s_visit_start_datetime"),
        ("STOP", "s_visit_end_datetime"),
        ("ENCOUNTERCLASS", "s_visit_type"),
        ("ENCOUNTERCLASS", encounter_type_mapper, {"mapped_value": "m_visit_type"}),
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

    ### Condition




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

    main(config_dict["csv_input_directory"], config_dict["csv_prepared_source_directory"], file_name_dict)

