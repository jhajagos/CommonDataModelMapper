from hi_classes import PHDPersonObject, PHFEncounterObject
from prepared_source_classes import SourcePersonObject
from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
    CoderMapperJSONClass, TransformMapper, FunctionMapper
from source_to_cdm_functions import generate_mapper_obj

import argparse
import json
import csv
import os
import logging

logging.basicConfig(level=logging.INFO)


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

    ["s_person_id", "s_gender", "m_gender", "s_birth_datetime", "s_death_datetime", "s_race",
     "m_race", "s_ethnicity", "m_ethnicity", "k_location"]

    ph_f_person_rules = [("empi_id", "s_person_id"),
                         ("birth_date", "s_birth_datetime"),
                         ("gender_display", "s_gender"),
                         ("gender_display", "m_gender"),
                         ("empi_id", person_race_code_mapper, {"description": "m_race"}),
                         ("empi_id", person_race_code_mapper, {"code": "s_race"}),
                         ("empi_id", person_ethnicity_code_mapper, {"description": "m_ethnicity"}),
                         ("empi_id", person_ethnicity_code_mapper, {"code": "s_ethnicity"}),
                         ("deceased_dt_tm", "s_death_datetime")
                       ]

    source_person_runner_obj = generate_mapper_obj(input_person_csv, PHDPersonObject(), output_person_csv,
                                                   SourcePersonObject(), ph_f_person_rules,
                                                   output_class_obj, in_out_map_obj)

    source_person_runner_obj.run()

    # Extract care sites

    encounter_csv = os.path.join(input_csv_directory, "PH_F_Encounter.csv")
    lookup_csv = os.path.join(input_csv_directory, "hi_care_location.csv")

    build_name_lookup_csv(encounter_csv, lookup_csv, ["facility", "hospital_service_code", "hospital_service_display",
                                                      "hospital_service_coding_system_id"],
                          ["facility", "hospital_service_display"])






def build_json_person_attribute(person_attribute_filename, attribute_json_file_name, sequence_field_name,
                                code_field_name, description_field_name,
                                descriptions_to_ignore=["Other", "Patient data refused", "Unknown",
                                                        "Ethnic group not given - patient refused", ""],
                                output_directory="./"):

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


def build_key_func_dict(fields, hash_func=None, separator="|"):
    if fields.__class__ not in ([].__class__, ().__class__):
        fields = [fields]

    def hash_func(input_dict):
        key_string = ""
        for field in fields:
            key_string += input_dict[field] + separator

        key_string = key_string[:-1 * len(separator)]
        if key_string[0:len(separator)] == separator:
            key_string = key_string[len(separator):]

        if hash_func is None:
            key_string = hash_func(key_string)

        return key_string

    return hash_func


def build_name_lookup_csv(input_csv_file_name, output_csv_file_name, field_names, key_fields):

    lookup_dict = {}

    key_func = build_key_func_dict(key_fields)

    with open(input_csv_file_name, "rb") as f:
        csv_dict = csv.DictReader(f)

        for row_dict in csv_dict:
            key_str = key_func(row_dict)
            new_dict = {}
            for field_name in field_names:
                new_dict[field_name] = row_dict[field_name]

            lookup_dict[key_str] = new_dict

    with open(output_csv_file_name, "wb") as fw:
        csv_writer = csv.writer(fw)

        i = 0
        for key_name in lookup_dict:

            row_dict = lookup_dict[key_name]
            if i == 0:
                row_field_names = row_dict.keys()
                header = ["key_name"] + row_field_names

                csv_writer.writerow(header)

            row_to_write = [key_name]
            for field_name in field_names:
                row_to_write += [row_dict[field_name]]

            csv_writer.writerow(row_to_write)

            i += 1

    return FunctionMapper(key_func)


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser()
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="hi_config.json")
    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as f:
        config_dict = json.load(f)

    main(config_dict["csv_input_directory"], config_dict["csv_output_directory"])