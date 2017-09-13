from hi_classes import *
from prepared_source_classes import *
from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
    CoderMapperJSONClass
from source_to_cdm_functions import build_input_output_mapper

import argparse
import json
import csv
import os


def main(input_csv_directory, output_csv_directory):

    output_class_obj = OutputClassDirectory()
    in_out_map_obj = InputOutputMapperDirectory()
    output_directory_obj = OutputClassDirectory()

    input_person_csv = os.path.join(input_csv_directory, "PH_D_Person.csv")
    output_person_csv = os.path.join(output_csv_directory, "source_person.csv")

    person_race_csv = os.path.join(input_csv_directory, "PH_D_Person_Race.csv")
    person_demographic_csv = os.path.join(input_csv_directory, "PH_D_Person_Demographic.csv")

    build_json_person_attribute(person_race_csv, "person_race.json", "person_seq", "race_code", "race_primary_display",
                                output_directory=input_csv_directory)

    build_json_person_attribute(person_demographic_csv, "person_ethnicity.json", "person_seq", "ethnicity_code",
                                "ethnicity_primary_display",
                                output_directory=input_csv_directory)

    person_race_code_mapper = CoderMapperJSONClass(os.path.join(input_csv_directory, "person_race.json"))

    person_ethnicity_code_mapper = CoderMapperJSONClass(os.path.join(input_csv_directory, "person_ethnicity.json"))

    ph_f_person_map = [("empi_id", "s_person_id"),
                       ("birth_date", "s_"),
                       ("gender", "s_gender"),
                       ]


def build_json_person_attribute(person_attribute_filename, attribute_json_file_name, sequence_field_name, code_field_name, description_field_name,
                                descriptions_to_ignore=["Other", "Patient data refused", "Unknown", "Ethnic group not given - patient refused", ""], output_directory="./"):

    """Due to that a Person can have multiple records for ethnicity and race we need to create a lookup"""

    master_attribute_dict = {}
    with open(person_attribute_filename, "rb") as f:

        csv_dict_reader = csv.DictReader(f)

        for row_dict in csv_dict_reader:
            master_patient_id = row_dict["empi_id"]
            sequence_id = row_dict[sequence_field_name]
            code = row_dict[code_field_name]
            code_description = row_dict[description_field_name]

            if code_description not in descriptions_to_ignore:

                record_attributes = {"sequence_id": sequence_id, "code": code, "description": code_description}

                if master_patient_id in master_attribute_dict:
                    master_attribute_dict[master_patient_id] += [record_attributes]
                else:
                    master_attribute_dict[master_patient_id] = [record_attributes]

        final_attribute_dict = {}
        for master_patient_id in master_attribute_dict:

            attribute_records = master_attribute_dict[master_patient_id]

            attribute_records.sort(key=lambda x: int(x["sequence_id"]))

            final_attribute_dict[master_patient_id] = attribute_records[0]

        full_attribute_json_file_name = os.path.join(output_directory, attribute_json_file_name)

        with open(full_attribute_json_file_name, "w") as fw:
            json.dump(final_attribute_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser()
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="hi_config.json")
    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as f:
        config_dict = json.load(f)

    main(config_dict["csv_input_directory"], config_dict["csv_output_directory"])