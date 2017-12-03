import sys, os
from .prepared_source_functions import build_name_lookup_csv, build_key_func_dict

try:
    from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
        CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper, ChainMapper, CascadeKeyMapper, \
        CascadeMapper, KeyTranslator, PassThroughFunctionMapper, CodeMapperDictClass
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, "src")))
    from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
        CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper, ChainMapper, CascadeKeyMapper, \
        CascadeMapper, KeyTranslator, PassThroughFunctionMapper, CodeMapperDictClass

from .hi_classes import PHDPersonObject, PHFEncounterObject, HiCareSite, EmpIdObservationPeriod, \
    PHFEncounterBenefitCoverage, PHFResultObject, PHFConditionObject, PHFProcedureObject, PHFMedicationObject

from .prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
    SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
    SourceProcedureObject, SourceMedicationObject

from .source_to_cdm_functions import generate_mapper_obj, create_json_map_from_csv_file

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
    person_demographic_csv = os.path.join(input_csv_directory, "PH_D_Person_Demographics.csv")

    build_json_person_attribute(person_race_csv, "person_race.json", "person_seq", "race_code", "race_primary_display",
                                output_directory=input_csv_directory)

    build_json_person_attribute(person_demographic_csv, "person_ethnicity.json", "person_seq", "ethnicity_code",
                                "ethnicity_primary_display",
                                output_directory=input_csv_directory)

    person_race_code_mapper = CoderMapperJSONClass(os.path.join(input_csv_directory, "person_race.json"))

    person_ethnicity_code_mapper = CoderMapperJSONClass(os.path.join(input_csv_directory, "person_ethnicity.json"))

    def has_date_func(input_dict):
        if input_dict["birth_date"] == "":
            return {"i_exclude": 1}
        else:
            return {}

    ph_f_person_rules = [("empi_id", "s_person_id"),
                         ("birth_date", "s_birth_datetime"),
                         ("gender_display", "s_gender"),
                         ("gender_display", "m_gender"),
                         ("empi_id", person_race_code_mapper, {"description": "m_race"}),
                         ("empi_id", person_race_code_mapper, {"code": "s_race"}),
                         ("empi_id", person_ethnicity_code_mapper, {"description": "m_ethnicity"}),
                         ("empi_id", person_ethnicity_code_mapper, {"code": "s_ethnicity"}),
                         ("deceased_dt_tm", "s_death_datetime"),
                         ("birth_date", PassThroughFunctionMapper(has_date_func), {"i_exclude": "i_exclude"})
                         ]

    source_person_runner_obj = generate_mapper_obj(input_person_csv, PHDPersonObject(), output_person_csv,
                                                   SourcePersonObject(), ph_f_person_rules,
                                                   output_class_obj, in_out_map_obj)

    source_person_runner_obj.run()

    # Extract care sites
    encounter_csv = os.path.join(input_csv_directory, "PH_F_Encounter.csv")
    care_site_csv = os.path.join(input_csv_directory, "hi_care_site.csv")

    md5_func = lambda x: hashlib.md5(x).hexdigest()
    # md5_func = None

    key_care_site_mapper = build_name_lookup_csv(encounter_csv, care_site_csv,
                                                 ["facility", "hospital_service_code", "hospital_service_display",
                                                  "hospital_service_coding_system_id"],
                                                 ["facility", "hospital_service_display"], hashing_func=md5_func)

    care_site_name_mapper = FunctionMapper(
        build_key_func_dict(["facility", "hospital_service_display"], separator=" - "))

    care_site_rules = [("key_name", "k_care_site"),
                       (("hospital_service_display", "hospital_service_code", "facility"),
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

    observation_runner_obj = generate_mapper_obj(hi_observation_period_csv, EmpIdObservationPeriod(),
                                                 source_observation_period_csv,
                                                 SourceObservationPeriodObject(), observation_period_rules,
                                                 output_class_obj, in_out_map_obj)

    observation_runner_obj.run()

    discharge_disposition_dict = {
        "Skilled Nursing": "Skilled Nursing Facility",
        "Other Death No Autopsy": "397709008",
        "Rehab Facility/Unit w/ planned readmit": "Comprehensive Inpatient Rehabilitation Facility",
        "Left Against Medical Advice": "225928004",
        "Transfer to a Rehabilitation Facility": "Comprehensive Inpatient Rehabilitation Facility",
        "Acute Hospital": "Inpatient Hospital",
        "Other Death Autopsy Unknown": "397709008",
        "Transfer to a Psychiatric Facility": "Inpatient Psychiatric Facility",
        "Discharged to skilled nursing facility for skilled care (SNF)": "Skilled Nursing Facility",
        "Home Hospice": "Hospice",
        "Hospice Facility": "Hospice",
        "Psych Hospital/Unit w/ planned readmit": "Inpatient Psychiatric Facility",
        "Rehab": "Comprehensive Inpatient Rehabilitation Facility",
        "Inpatient Rehabilitation Facility": "Comprehensive Inpatient Rehabilitation Facility",
        "Transferred to a short term general hospital for inpatient care": "Inpatient Hospital",
        "Other Death Autopsy Performed": "397709008",
        "Expired": "397709008",
        "Non Srg Dth W/In 48h of Adm No Autopsy": "397709008",
        "Left against medical advice or discontinued care": "225928004",
        "Transferred to Stony Brook for Inpatient Care": "Inpatient Hospital",
        "Long Term Care Facility": "Inpatient Long-term Care",
        "Federal Hospital": "Inpatient Hospital",
        "Non Srg Dth W/In 48h of Adm Autopsy Unk": "397709008",
        "Long Term Care": "Inpatient Long-term Care",
        "Srg Dth W/In 48h Post Srg No Autopsy": "397709008",
        "Srg Dth W/In 3-10dy Post Srg No Autopsy": "397709008",
        "Srg Dth W/In 3-10dy Post Srg Autopsy Unk": "397709008",
        "Discharged home with hospice care": "Hospice",
        "Srg Dth W/In 48h Post Srg Autopsy Unk": "397709008",
        "Died in Operating Room No Autopsy": "397709008",
        "Died in Operating Room Autopsy Unknown": "397709008",
        "Non Srg Dth W/In 48h of Adm Autopsy Per": "397709008",
        "Srg Dth W/In 48h Post Srg Autopsy Per": "397709008",
        "Srg Dth W/In 3-10dy Post Srg Autopsy Per": "397709008"
    }

    discharge_disposition_mapper = CodeMapperDictClass(discharge_disposition_dict, "discharge_disposition_display",
                                                       "m_discharge_to")

    admit_source_dict = {"EO:  Emergency OP Unit": "Emergency Room - Hospital",
                         "Routine Admission": "3241000175106",
                         "Newborn": "3241000175106",
                         "Emergency Department": "Emergency Room - Hospital",
                         "TH: Transfer From A Hospital": "Inpatient Hospital",
                         "ER": "Emergency Room - Hospital",
                         "Transfer from a Hospital": "Inpatient Hospital",
                         "Transfer from Trans-Skilled Nursing Fac": "Skilled Nursing Facility",
                         "NS: Newborn Sick": "3241000175106",
                         "Normal Delivery": "3241000175106",
                         "NP: Newborn Premature": "3241000175106",
                         "Transfer from Long Island Veteran's Home": "Skilled Nursing Facility",
                         "Newborn Transfer": "Inpatient Hospital",
                         "Transfer from Psychiatric Facility": "Inpatient Psychiatric Facility",
                         "Trans from other Hospital": "Inpatient Hospital",
                         "Transfer from Hospice": "Hospice",
                         "Hospice": "Hospice",
                         "Extramural Delivery": "Skilled Nursing Facility"}

    admit_source_mapper = CodeMapperDictClass(admit_source_dict, "admission_source_display", "m_admitting_source")

    ph_f_encounter_csv = os.path.join(input_csv_directory, "PH_F_Encounter.csv")
    source_encounter_csv = os.path.join(output_csv_directory, "source_encounter.csv")

    def visit_occurrence_i_exclude(input_dict):
        if "classification_display" in input_dict:
            if input_dict["classification_display"] == "Inbox Message":
                return {"i_exclude": 1}
            else:
                return {}
        else:
            return {}

    encounter_rules = [("encounter_id", "s_encounter_id"),
                       ("empi_id", "s_person_id"),
                       ("service_dt_tm", "s_visit_start_datetime"),
                       ("discharge_dt_tm", "s_visit_end_datetime"),
                       ("classification_display", "s_visit_type"),
                       ("classification_display", "m_visit_type"),
                       (("facility", "hospital_service_display"),
                        key_care_site_mapper, {"mapped_value": "k_care_site"}),
                       ("discharge_disposition_display", "s_discharge_to"),
                       ("discharge_disposition_display", discharge_disposition_mapper,
                        {"m_discharge_to": "m_discharge_to"}),
                       ("admission_source_display", "s_admitting_source"),
                       ("admission_source_display", admit_source_mapper, {"m_admitting_source": "m_admitting_source"}),
                       ("classification_display", PassThroughFunctionMapper(visit_occurrence_i_exclude),
                        {"i_exclude": "i_exclude"})]

    visit_runner_obj = generate_mapper_obj(ph_f_encounter_csv, PHFEncounterObject(), source_encounter_csv,
                                           SourceEncounterObject(),
                                           encounter_rules, output_class_obj, in_out_map_obj)

    visit_runner_obj.run()

    # Benefit Coverage

    ph_f_encounter_benefit_coverage_csv = os.path.join(input_csv_directory, "PH_F_Encounter_Benefit_Coverage.csv")
    source_encounter_coverage_csv = os.path.join(output_csv_directory, "source_encounter_coverage.csv")

    encounter_coverage_rules = [("empi_id", "s_person_id"),
                                ("encounter_id", "s_encounter_id"),
                                ("begin_dt_tm", "s_start_payer_date"),
                                (("end_dt_tm", "begin_dt_tm"),
                                 FilterHasKeyValueMapper(["end_dt_tm", "begin_dt_tm"], empty_value="0"),
                                 {"end_dt_tm": "s_end_payer_date", "begin_dt_tm": "s_end_payer_date"}),
                                ("payer_name", "s_payer_name"),
                                ("payer_name", "m_payer_name"),
                                ("plan_name", "s_plan_name"),
                                ("benefit_type_primary_display", "m_plan_name")]

    encounter_benefit_runner_obj = generate_mapper_obj(ph_f_encounter_benefit_coverage_csv,
                                                       PHFEncounterBenefitCoverage(),
                                                       source_encounter_coverage_csv, SourceEncounterCoverageObject(),
                                                       encounter_coverage_rules, output_class_obj, in_out_map_obj)

    encounter_benefit_runner_obj.run()

    ph_f_result_csv = os.path.join(input_csv_directory, "PH_F_Result.csv")

    source_result_csv = os.path.join(output_csv_directory, "source_result.csv")

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
                    ("norm_unit_of_measure_display", "s_result_unit"),
                    ("norm_unit_of_measure_code", "s_result_unit_code"),
                    ("norm_ref_range_low", "s_result_numeric_lower"),
                    ("norm_ref_range_high", "s_result_numeric_upper")]

    result_mapper_obj = generate_mapper_obj(ph_f_result_csv, PHFResultObject(), source_result_csv, SourceResultObject(),
                                            result_rules, output_class_obj, in_out_map_obj)

    result_mapper_obj.run()

    # Claim IDs
    map_claim_id_encounter = os.path.join(input_csv_directory, "Map_Between_Claim_Id_Encounter_Id.csv")
    map_claim_id_encounter_json = create_json_map_from_csv_file(map_claim_id_encounter, "claim_uid", "encounter_id")
    claim_id_encounter_id_mapper = CoderMapperJSONClass(map_claim_id_encounter_json, "claim_id")

    encounter_id_claim_id_mapper = CascadeMapper(FilterHasKeyValueMapper(["encounter_id"]),
                                                 ChainMapper(FilterHasKeyValueMapper(["claim_id"]),
                                                             claim_id_encounter_id_mapper))

    def s_condition_type_func(input_dict):

        if input_dict["supporting_fact_type"] == "CLAIM":
            if input_dict["source_description"] == "Siemens":
                if len(input_dict["present_on_admission_raw_code"]):
                    return {"s_condition_type": "Final"}
                else:
                    return {"s_condition_type": "Admitting"}
            else:
                if input_dict["classification_primary_display"] == "Admitting":
                    return {"s_condition_type": "Admitting"}
                else:
                    return {"s_condition_type": "Final"}

        else:
            if input_dict["classification_primary_display"] == "Final diagnosis (discharge)":
                return {"s_condition_type": "Final"}
            else:
                if input_dict["confirmation_status_display"] == "Confirmed":
                    return {"s_condition_type": input_dict["classification_primary_display"]}
                else:
                    return {"s_condition_type": "Preliminary"}

    def m_rank_func(input_dict):
        if input_dict["rank_type"] == "PRIMARY":
            return {"m_rank": "Primary"}
        elif input_dict["rank_type"] == "SECONDARY":
            return {"m_rank": "Secondary"}
        else:
            return {}

    condition_rules = [("empi_id", "s_person_id"),
                       (("encounter_id", "claim_id"), encounter_id_claim_id_mapper, {"encounter_id": "s_encounter_id"}),
                       ("effective_dt_tm", "s_start_condition_datetime"),
                       ("condition_code", "s_condition_code"),
                       ("condition_coding_system_id", "m_condition_code_oid"),
                       ("rank_type", PassThroughFunctionMapper(m_rank_func), {"m_rank": "m_rank"}),
                       (("supporting_fact_type", "classification_primary_display", "confirmation_status_display",
                         "present_on_admission_raw_code", "source_description"),
                        PassThroughFunctionMapper(s_condition_type_func), {"s_condition_type": "s_condition_type"}),
                       ("present_on_admission_raw_code", "s_present_on_admission_indicator")]

    ph_f_condition_csv = os.path.join(input_csv_directory, "PH_F_Condition.csv")
    source_condition_csv = os.path.join(output_csv_directory, "source_condition.csv")
    condition_mapper_obj = generate_mapper_obj(ph_f_condition_csv, PHFConditionObject(), source_condition_csv,
                                               SourceConditionObject(),
                                               condition_rules, output_class_obj, in_out_map_obj)

    condition_mapper_obj.run()

    procedure_rules = [("empi_id", "s_person_id"),
                       (("encounter_id", "claim_id"), encounter_id_claim_id_mapper, {"encounter_id": "s_encounter_id"}),
                       ("procedure_code", "s_procedure_code"),
                       ("procedure_coding_system_id", "m_procedure_code_oid"),
                       ("service_start_dt_tm", "s_start_procedure_datetime"),
                       ("rank_type", "s_rank")]

    ph_f_procedure_csv = os.path.join(input_csv_directory, "PH_F_Procedure.csv")
    source_procedure_csv = os.path.join(output_csv_directory, "source_procedure.csv")

    procedure_mapper_obj = generate_mapper_obj(ph_f_procedure_csv, PHFProcedureObject(), source_procedure_csv,
                                               SourceProcedureObject(),
                                               procedure_rules, output_class_obj, in_out_map_obj)

    procedure_mapper_obj.run()

    def active_medications(input_dict):
        if "status_primary_display" in input_dict:
            if input_dict["status_primary_display"] not in ('Complete', 'Discontinued', 'Active', 'Suspended'):
                return {"i_exclude": 1}
            else:
                return {}
        else:
            return {}

    medication_rules = [("empi_id", "s_person_id"),
                        ("encounter_id", "s_encounter_id"),
                        ("drug_code", "s_drug_code"),
                        ("drug_raw_coding_system_id", "m_drug_code_oid"),
                        ("drug_primary_display", "s_drug_text"),
                        ("start_dt_tm", "s_start_medication_datetime"),
                        ("stop_dt_tm", "s_end_medication_datetime"),
                        ("route_display", "s_route"),
                        ("dose_quantity", "s_quantity"),
                        ("dose_unit_display", "s_dose_unit"),
                        ("intended_dispenser", "s_drug_type"),
                        ("status_display", "s_status"),
                        ("status_primary_display", PassThroughFunctionMapper(active_medications),
                         {"i_exclude": "i_exclude"})
                        ]

    ph_f_medication_csv = os.path.join(input_csv_directory, "PH_F_Medication.csv")
    source_medication_csv = os.path.join(output_csv_directory, "source_medication.csv")

    medication_mapper_obj = generate_mapper_obj(ph_f_medication_csv, PHFMedicationObject(), source_medication_csv,
                                                SourceMedicationObject(), medication_rules,
                                                output_class_obj, in_out_map_obj)

    medication_mapper_obj.run()


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


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser()
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="hi_config.json")
    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as f:
        config_dict = json.load(f)

    main(config_dict["csv_input_directory"], config_dict["csv_input_directory"])