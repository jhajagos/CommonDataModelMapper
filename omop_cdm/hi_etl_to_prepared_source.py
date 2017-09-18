from hi_classes import PHDPersonObject, PHFEncounterObject, HiCareSite, EmpIdObservationPeriod, \
    PHFEncounterBenefitCoverage, PHFResultObject
from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
    SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject
from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
    CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper
from source_to_cdm_functions import generate_mapper_obj

import argparse
import json
import csv
import os
import logging
import hashlib

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
    care_site_csv = os.path.join(input_csv_directory, "hi_care_site.csv")

    md5_func = lambda x: hashlib.md5(x).hexdigest()
    md5_func = None

    key_care_site_mapper = build_name_lookup_csv(encounter_csv, care_site_csv,
                                                 ["facility", "hospital_service_code", "hospital_service_display",
                                                  "hospital_service_coding_system_id"],
                                                 ["facility", "hospital_service_display"], hashing_func=md5_func)

    care_site_name_mapper = FunctionMapper(build_key_func_dict(["facility", "hospital_service_display"], separator=" - "))

    care_site_rules = [("key_name","k_care_site"),
                       (("hospital_service_display", "hospital_service_code","facility"),
                        care_site_name_mapper,
                        {"mapped_value": "s_care_site_name"})]

    source_care_site_csv = os.path.join(output_csv_directory, "source_care_site.csv")

    care_site_runner_obj = generate_mapper_obj(care_site_csv, HiCareSite(), source_care_site_csv,
                                               SourceCareSiteObject(), care_site_rules,
                                               output_class_obj, in_out_map_obj)

    care_site_runner_obj.run()

    hi_observation_period_csv = os.path.join(input_csv_directory, "EMPI_ID_Observation_Period.csv")

    observation_period_rules = [("empi_id", "s_person_id"),
                                ("min_service_dt_tm", "s_start_observation_datetime"),
                                ("max_service_dt_tm", "s_end_observation_datetime")]

    source_observation_period_csv = os.path.join(output_csv_directory, "source_observation_period.csv")

    observation_runner_obj = generate_mapper_obj(hi_observation_period_csv, EmpIdObservationPeriod(), source_observation_period_csv,
                                SourceObservationPeriodObject(), observation_period_rules,
                                output_class_obj, in_out_map_obj)

    observation_runner_obj.run()

    ["s_encounter_id", "s_person_id", "s_visit_start_datetime", "s_visit_end_datetime", "s_visit_type", "m_visit_type",
     "k_care_site", "i_exclude"]

    ph_f_encounter_csv = os.path.join(input_csv_directory, "PH_F_Encounter.csv")
    source_encounter_csv = os.path.join(output_csv_directory, "source_encounter.csv")

    encounter_rules = [("encounter_id", "s_encounter_id"),
                       ("empi_id", "s_person_id"),
                       ("service_dt_tm", "s_visit_start_datetime"),
                       ("discharge_dt_tm", "s_visit_end_datetime"),
                       ("classification_display", "s_visit_type"),
                       ("classification_display", "m_visit_type"),
                       (("facility", "hospital_service_display"),
                        key_care_site_mapper, {"mapped_value": "k_care_site"})
                      ]

    visit_runner_obj = generate_mapper_obj(ph_f_encounter_csv, PHFEncounterObject(), source_encounter_csv, SourceEncounterObject(),
                                           encounter_rules, output_class_obj, in_out_map_obj)

    visit_runner_obj.run()

    # Benefit Coverage

    ph_f_encounter_benefit_coverage_csv = os.path.join(input_csv_directory, "PH_F_Encounter_Benefit_Coverage.csv")
    source_encounter_coverage_csv = os.path.join(output_csv_directory, "source_encounter_coverage.csv")

    encounter_coverage_rules = [("empi_id", "s_person_id"),
                               ("encounter_id", "s_encounter_id"),
                               ("begin_dt_tm","s_start_payer_date"),
                               ("end_dt_tm", FilterHasKeyValueMapper(["end_dt_tm", "begin_dt_tm"], empty_value="0"),
                                {"end_dt_tm": "s_end_payer_date", "begin_dt_tm": "s_end_payer_date"}),
                               ("payer_name", "s_payer_name"),
                               ("payer_name", "m_payer_name"),
                               ("plan_name", "s_plan_name"),
                               ("benefit_type_primary_display", "m_plan_name")]

    encounter_benefit_runner_obj = generate_mapper_obj(ph_f_encounter_benefit_coverage_csv, PHFEncounterBenefitCoverage(),
                                                       source_encounter_coverage_csv, SourceEncounterCoverageObject(),
                                                       encounter_coverage_rules, output_class_obj, in_out_map_obj)

    encounter_benefit_runner_obj.run()

    ph_f_result_csv = os.path.join(input_csv_directory, "PH_F_Result.csv")

    source_result_csv = os.path.join(output_csv_directory, "source_result.csv")

    """
    measurement_rules = [(":row_id", "measurement_id"),
                         ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                         ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                         ("service_date", SplitDateTimeWithTZ(),
                          {"date": "measurement_date", "time": "measurement_time"}),
                         ("result_code", "measurement_source_value"),
                         ("result_code", measurement_code_mapper, {"CONCEPT_ID": "measurement_source_concept_id"}),
                         ("result_code", measurement_code_mapper, {"CONCEPT_ID": "measurement_concept_id"}),
                         (
                         "result_code", measurement_type_chained_mapper, {"CONCEPT_ID": "measurement_type_concept_id"}),
                         ("norm_numeric_value", FloatMapper(), "value_as_number"),
                         (("norm_codified_value_code", "interpretation_primary_display", "norm_text_value"),
                          value_as_concept_mapper, {"CONCEPT_ID": "value_as_concept_id"}),
                         # norm_codified_value_primary_display",
                         ("norm_unit_of_measure_primary_display", "unit_source_value"),
                         ("norm_unit_of_measure_code", unit_measurement_mapper, {"CONCEPT_ID": "unit_concept_id"}),
                         (("norm_numeric_value", "norm_codified_value_primary_display", "result_primary_display",
                           "norm_text_value"),
                          numeric_coded_mapper,  # ChainMapper(numeric_coded_mapper, LeftMapperString(50)),
                          {"norm_numeric_value": "value_source_value",
                           "norm_codified_value_primary_display": "value_source_value",
                           "result_primary_display": "value_source_value",
                           "norm_text_value": "value_source_value"}),
                         ("norm_ref_range_low", FloatMapper(), "range_low"),
                         # TODO: Some values contain non-numeric elements
                         ("norm_ref_range_high", FloatMapper(), "range_high")]
    """

    ["s_person_id", "s_encounter_id", "s_obtained_datetime", "s_type_name", "s_type_code", "m_type_code_oid",
    "s_result_text", "s_result_numeric", "s_result_datetime", "s_result_code", "m_result_code_oid",
    "s_result_unit", "s_result_unit_code", "m_result_unit_code_oid",
    "s_result_numeric_lower", "s_result_numeric_upper", "i_exclude"]

    result_rules = [("empi_id", "s_person_id"),
                    ("encounter_id", "s_encounter_id"),
                    ("service_date", "s_obtained_datetime"),
                    ("result_display", "s_type_name"),
                    ("result_code", "s_type_code"),
                    ("result_coding_system_id", "m_type_code_oid"),
                    (("norm_codified_value_primary_display", "result_primary_display",
                           "norm_text_value"), FilterHasKeyValueMapper(["norm_codified_value_primary_display",
                                                                         "norm_text_value"]),
                     {"norm_codified_value_primary_display": "s_result_text",
                      "norm_text_value": "s_result_text"}),
                    ("norm_numeric_value", "s_result_numeric"),
                    ("norm_date_value", "s_result_datetime"),
                    (("norm_codified_value_code", "interpretation_primary_display"),
                     FilterHasKeyValueMapper(["norm_codified_value_code", "interpretation_primary_display"]),
                     {"norm_codified_value_code": "s_result_code", "interpretation_primary_display": "s_result_code"}),
                    ("norm_unit_of_measure_display","s_result_unit"),
                    ("norm_unit_of_measure_code", "s_result_unit_code"),
                    ("norm_ref_range_low", "s_result_numeric_lower"),
                    ("norm_ref_range_high", "s_result_numeric_upper")]



    result_mapper_obj = generate_mapper_obj(ph_f_result_csv, PHFResultObject(), source_result_csv, SourceResultObject(),
                                            result_rules,  output_class_obj, in_out_map_obj)

    result_mapper_obj.run()






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


def build_key_func_dict(fields, hashing_func=None, separator="|"):
    if fields.__class__ not in ([].__class__, ().__class__):
        fields = [fields]

    def hash_func(input_dict):
        key_list = []
        for field in fields:
            key_list += [input_dict[field]]

        key_list = [kl for kl in key_list if len(kl)]
        key_string = separator.join(key_list)

        if hashing_func is not None:
            key_string = hashing_func(key_string)

        return key_string

    return hash_func


def build_name_lookup_csv(input_csv_file_name, output_csv_file_name, field_names, key_fields, hashing_func=None):

    lookup_dict = {}

    key_func = build_key_func_dict(key_fields, hashing_func=hashing_func)

    with open(input_csv_file_name, "rb") as f:
        csv_dict = csv.DictReader(f)

        for row_dict in csv_dict:
            key_str = key_func(row_dict)
            new_dict = {}
            for field_name in field_names:
                new_dict[field_name] = row_dict[field_name]

            if hashing_func is not None:
                key_str = hashing_func(key_str)

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

            if len(key_name):
                row_to_write = [key_name]
                for field_name in row_field_names:
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

    main(config_dict["csv_input_directory"], config_dict["csv_input_directory"])