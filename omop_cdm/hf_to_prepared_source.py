import argparse
import json
import os
import csv
import datetime

from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
    SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
    SourceProcedureObject, SourceMedicationObject


from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
    SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
    SourceProcedureObject, SourceMedicationObject


def generate_patient_csv_file(patient_encounter_csv_file_name, output_directory):

    patient_fields = ["marital_status", "patient_id", "race", "gender", "patient_sk"]
    
    file_to_write = os.path.join(output_directory, "hf_patient.csv")
    file_to_read = patient_encounter_csv_file_name

    with open(file_to_read, "rb") as f:

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

        with open(file_to_write, "wb") as fw:
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

    generate_patient_csv_file(encounter_patient_file_name, input_csv_directory)


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



