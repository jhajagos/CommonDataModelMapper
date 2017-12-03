import argparse

import json
import os
import csv
import datetime
import hashlib

from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
        CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper, ChainMapper, CascadeKeyMapper, \
        CascadeMapper, KeyTranslator, PassThroughFunctionMapper, CodeMapperDictClass, CodeMapperDictClass

from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
    SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
    SourceProcedureObject, SourceMedicationObject

from source_to_cdm_functions import generate_mapper_obj
from hf_classes import HFPatient, HFCareSite, HFEncounter, HFObservationPeriod, HFDiagnosis
from prepared_source_functions import build_name_lookup_csv, build_key_func_dict


def generate_observation_period(encounter_csv_file_name, hf_period_observation_csv_file_name,
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

    with open(hf_period_observation_csv_file_name, "w", newline="") as fw:
        csv_writer = csv.writer(fw)

        csv_writer.writerow([id_field_name, start_date_field_name, end_date_field_name])

        for id_value in observation_period_dict:
            start_date_value, end_date_value = observation_period_dict[id_value]
            row_to_write = [id_value, start_date_value, end_date_value]
            csv_writer.writerow(row_to_write)


def generate_patient_csv_file(patient_encounter_csv_file_name, output_directory):
    """Create a patient CSV file from the encounter patient file"""

    patient_fields = ["marital_status", "patient_id", "race", "gender", "patient_sk"]
    
    file_to_write = os.path.join(output_directory, "hf_patient.csv")
    file_to_read = patient_encounter_csv_file_name

    with open(file_to_read, "r", newline="") as f:

        dict_reader = csv.DictReader(f)

        result_dict = {}
        for row_dict in dict_reader:

            admit_dt_tm_txt = row_dict["admitted_dt_tm"]
            if len(admit_dt_tm_txt):
                admit_dt_tm = datetime.datetime.strptime(admit_dt_tm_txt, "%Y-%m-%d %H:%M:%S")
                age_in_years = row_dict["age_in_years"]
                age_in_years_td = datetime.timedelta(int(age_in_years) * 365.25)
                estimated_dob_dt_tm = admit_dt_tm - age_in_years_td
                year_of_birth = estimated_dob_dt_tm.year
            else:
                age_in_years = None

            patient_id = row_dict["patient_id"]
            
            patient_dict = {}
            for field in patient_fields:
                patient_dict[field] = row_dict[field]

            patient_dict["year_of_birth"] = year_of_birth
            if patient_id not in result_dict:

                result_dict[patient_id] = patient_dict

            else:
                existing_patient_dict = result_dict[patient_id]
                existing_year_of_birth = existing_patient_dict["year_of_birth"]
                if year_of_birth < existing_year_of_birth or year_of_birth is None:
                    result_dict[patient_id] = patient_dict

        with open(file_to_write, "w", newline="") as fw:
            fields_to_write = patient_fields + ["year_of_birth"]
            csv_writer = csv.writer(fw)
            csv_writer.writerow(fields_to_write)

            for patient_id in result_dict:
                patient_dict = result_dict[patient_id]
                row_to_write = []
                for field in fields_to_write:
                    row_to_write += [patient_dict[field]]
                csv_writer.writerow(row_to_write)

        return file_to_write


def main(input_csv_directory, output_csv_directory, file_name_dict):

    encounter_file_name = os.path.join(input_csv_directory, file_name_dict["encounter"])
    encounter_patient_file_name = os.path.join(input_csv_directory, file_name_dict["encounter_patient"])

    patient_file_name = generate_patient_csv_file(encounter_patient_file_name, input_csv_directory)

    file_name_dict["patient"] = patient_file_name
    print(file_name_dict)

    output_person_csv = os.path.join(output_csv_directory, "source_person.csv")

    output_class_obj = OutputClassDirectory()
    in_out_map_obj = InputOutputMapperDirectory()

    race_map = {
                "African American": "African American",
                "Asian": "Asian",
                #Biracial
                "Caucasian": "White",
                #Hispanic - ethnicity
                #Mid Eastern Indian
                "Native American": "American Indian or Alaska Native",
                #Not Mapped
                #NULL
                #Other
                "Pacific Islander": "Native Hawaiian or Other Pacific Islander"
                #Unknown
    }
    race_mapper = CodeMapperDictClass(race_map)

    ethnicity_source_map = {"Hispanic": "Hispanic"}
    ethnicity_source_mapper = CodeMapperDictClass(ethnicity_source_map)

    ethnicity_map = {"Hispanic": "Hispanic or Latino"}
    ethnicity_mapper = CodeMapperDictClass(ethnicity_map)

    hf_patient_rules = [("patient_id", "s_person_id"),
                        ("gender", "s_gender"),
                        (("year_of_birth",), FunctionMapper(lambda x: x["year_of_birth"] + '-01-01', "date_of_birth"),
                        {"date_of_birth": "s_birth_datetime"}),
                        ("race", "s_race"),
                        ("race", race_mapper, {"mapped_value": "m_race"}),
                        ("race", ethnicity_source_mapper, {"mapped_value": "s_ethnicity"}),
                        ("race", ethnicity_mapper, {"mapped_value": "m_ethnicity"})]

    source_person_runner_obj = generate_mapper_obj(file_name_dict["patient"], HFPatient(), output_person_csv,
                                                   SourcePersonObject(), hf_patient_rules,
                                                   output_class_obj, in_out_map_obj)

    source_person_runner_obj.run()

    # Observation Period

    hf_observation_period_csv = os.path.join(input_csv_directory, "hf_observation_period.csv")
    generate_observation_period(encounter_file_name, hf_observation_period_csv,
                                "patient_id", "admitted_dt_tm", "discharged_dt_tm")

    observation_period_rules = [("patient_id", "s_person_id"),
                                ("admitted_dt_tm", "s_start_observation_datetime"),
                                ("discharged_dt_tm", "s_end_observation_datetime")]

    source_observation_period_csv = os.path.join(output_csv_directory, "source_observation_period.csv")

    observation_runner_obj = generate_mapper_obj(hf_observation_period_csv, HFObservationPeriod(),
                                                 source_observation_period_csv,
                                                 SourceObservationPeriodObject(), observation_period_rules,
                                                 output_class_obj, in_out_map_obj)
    observation_runner_obj.run()

    # Care site
    care_site_csv = os.path.join(input_csv_directory, "care_site.csv")

    md5_func = lambda x: hashlib.md5(x.encode("utf8")).hexdigest()

    key_care_site_mapper = build_name_lookup_csv(encounter_file_name, care_site_csv,
                                                 ["hospital_id", "caresetting_desc"],
                                                 ["hospital_id", "caresetting_desc"], hashing_func=md5_func)

    care_site_name_mapper = FunctionMapper(
        build_key_func_dict(["hospital_id", "caresetting_desc"], separator=" - "))

    care_site_rules = [("key_name", "k_care_site"),
                       (("hospital_id", "caresetting_desc"),
                         care_site_name_mapper,
                        {"mapped_value": "s_care_site_name"})]

    source_care_site_csv = os.path.join(output_csv_directory, "source_care_site.csv")

    care_site_runner_obj = generate_mapper_obj(care_site_csv, HFCareSite(), source_care_site_csv,
                                               SourceCareSiteObject(), care_site_rules,
                                               output_class_obj, in_out_map_obj)

    care_site_runner_obj.run()

    # Encounter

    ["s_encounter_id", "s_person_id", "s_visit_start_datetime", "s_visit_end_datetime", "s_visit_type",
     "m_visit_type", "k_care_site", "s_discharge_to", "m_discharge_to",
     "s_admitting_source", "m_admitting_source", "i_exclude"]

    encounter_rules = [
        ("encounter_id", "s_encounter_id"),
        ("patient_id", "s_person_id"),
        ("admitted_dt_tm", "s_visit_start_datetime"),
        ("discharged_dt_tm", "s_visit_end_datetime"),
        ("patient_type_desc", "s_visit_type"),
        ("patient_type_desc", "m_visit_type"),
        (("hospital_id", "caresetting_desc"), key_care_site_mapper, {"mapped_value": "k_care_site"}),
        ("dischg_disp_code_desc", "s_discharge_to"),
        #("", "m_discharge_to"),
        ("admission_source_code_desc", "s_admitting_source")
        #("m_admitting_source")
    ]

    source_encounter_csv = os.path.join(output_csv_directory, "source_encounter.csv")

    encounter_runner_obj = generate_mapper_obj(encounter_file_name, HFEncounter(), source_encounter_csv,
                                               SourceEncounterObject(), encounter_rules,
                                               output_class_obj, in_out_map_obj)

    encounter_runner_obj.run()

    # Encounter plan or insurance coverage

    source_encounter_coverage_csv = os.path.join(output_csv_directory, "source_encounter_coverage.csv")

    encounter_coverage_rules = [("patient_id", "s_person_id"),
                                ("encounter_id", "s_encounter_id"),
                                ("admitted_dt_tm", "s_start_payer_date"),
                                ("discharged_dt_tm", "s_end_payer_date"),
                                ("payer_code_desc", "s_payer_name"),
                                ("payer_code_desc", "m_payer_name"),
                                ("payer_code_desc", "s_plan_name"),
                                ("payer_code_desc", "m_plan_name")]

    encounter_benefit_runner_obj = generate_mapper_obj(encounter_file_name,
                                                       HFEncounter(),
                                                       source_encounter_coverage_csv, SourceEncounterCoverageObject(),
                                                       encounter_coverage_rules, output_class_obj, in_out_map_obj)

    encounter_benefit_runner_obj.run()

    # Diagnosis / condition

    dx_code_oid_map = {
        "ICD9": "2.16.840.1.113883.6.103"
    }

    dx_code_oid_mapper = CodeMapperDictClass(dx_code_oid_map)

    ["s_person_id", "s_encounter_id", "s_start_condition_datetime", "s_end_condition_datetime",
     "s_condition_code", "m_condition_code_oid", "s_sequence_id", "m_rank", "s_condition_type",
     "s_present_on_admission_indicator"]

    condition_rules = [("patient_id", "s_person_id"),
                       ("encounter_id", "s_encounter_id"),
                       ("diagnosis_code","s_condition_code"),
                       ("diagnosis_type", "s_condition_code_type"),
                       ("diagnosis_type", dx_code_oid_mapper, {"mapped_value": "m_condition_code_oid"}),
                       ("diagnosis_priority", "s_sequence_id"),
                       ("diagnosis_type_display", "s_condition_type"),
                       ("present_on_admit_code","s_present_on_admission_indicator"),
                       ("admitted_dt_tm", "s_start_condition_datetime"),
                       ("discharged_dt_tm", "s_end_condition_datetime")
                       ]

    hf_diagnosis_csv = os.path.join(input_csv_directory, file_name_dict["diagnosis"])
    source_condition_csv = os.path.join(output_csv_directory, "source_condition.csv")
    condition_mapper_obj = generate_mapper_obj(hf_diagnosis_csv, HFDiagnosis(), source_condition_csv,
                                               SourceConditionObject(),
                                               condition_rules, output_class_obj, in_out_map_obj)

    condition_mapper_obj.run()


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser()
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="hf_config.json")
    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as f:
        config_dict = json.load(f)

    file_name_dict = {
        "clinical_events": "healthfacts._clinical_event_joined_to_export_20171203_095405.csv",
        "diagnosis": "healthfacts._diagnosis_joined_to_export_20171203_095405.csv",
        "encounter": "healthfacts._encounter_patient_joined_to_export_20171203_095405.csv",
        "encounter_patient": "healthfacts._encounter_patient_joined_to_export_20171203_095405.csv",
        "lab_procedure": "healthfacts._lab_procedure_joined_to_export_20171203_095405.csv",
        "medication": "healthfacts._medication_joined_to_export_20171203_095405.csv",
        "procedure": "healthfacts._procedure_joined_to_export_20171203_095405.csv"
    }

    main(config_dict["csv_input_directory"], config_dict["csv_input_directory"], file_name_dict)