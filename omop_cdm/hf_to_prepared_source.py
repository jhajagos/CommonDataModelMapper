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
from hf_classes import HFPatient, HFCareSite
from prepared_source_functions import build_name_lookup_csv, build_key_func_dict


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

    source_care_site_csv = os.path.join(output_csv_directory, "source_care_site.csv")

    care_site_runner_obj = generate_mapper_obj(care_site_csv, HFCareSite(), source_care_site_csv,
                                               SourceCareSiteObject(), care_site_rules,
                                               output_class_obj, in_out_map_obj)

    care_site_runner_obj.run()


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser()
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="hf_config.json")
    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as f:
        config_dict = json.load(f)

    file_name_dict = {
        "clinical_events": "healthfacts._clinical_event_joined_to_export_20171127_174343.csv",
        "diagnosis": "healthfacts._diagnosis_joined_to_export_20161229_124107.csv",
        "encounter": "healthfacts._encounter_joined_to_export_20161229_124107.csv",
        "encounter_patient": "healthfacts._encounter_patient_joined_to_export_20161229_124107.csv",
        "lab_procedure": "healthfacts._lab_procedure_joined_to_export_20161229_124107.csv",
        "medication": "healthfacts._medication_joined_to_export_20161229_124107.csv",
        "procedure": "healthfacts._procedure_joined_to_export_20161229_124107.csv"
    }


    main(config_dict["csv_input_directory"], config_dict["csv_input_directory"], file_name_dict)



