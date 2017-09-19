from hi_classes import PHDPersonObject, PHFEncounterObject, HiCareSite, EmpIdObservationPeriod, \
    PHFEncounterBenefitCoverage, PHFResultObject, PHFConditionObject, PHFProcedureObject, PHFMedicationObject

from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
    SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
    SourceProcedureObject, SourceMedicationObject

from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
    CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper, ChainMapper, CascadeKeyMapper, \
    CascadeMapper, KeyTranslator, PassThroughFunctionMapper, CodeMapperDictClass

from source_to_cdm_functions import generate_mapper_obj, create_json_map_from_csv_file

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
    #md5_func = None

    key_care_site_mapper = build_name_lookup_csv(encounter_csv, care_site_csv,
                                                 ["facility", "hospital_service_code", "hospital_service_display",
                                                  "hospital_service_coding_system_id"],
                                                 ["facility", "hospital_service_display"], hashing_func=md5_func)

    care_site_name_mapper = FunctionMapper(build_key_func_dict(["facility", "hospital_service_display"], separator=" - "))

    care_site_rules = [("key_name","k_care_site"),
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

    observation_runner_obj = generate_mapper_obj(hi_observation_period_csv, EmpIdObservationPeriod(), source_observation_period_csv,
                                SourceObservationPeriodObject(), observation_period_rules,
                                output_class_obj, in_out_map_obj)

    observation_runner_obj.run()

    ["s_encounter_id", "s_person_id", "s_visit_start_datetime", "s_visit_end_datetime", "s_visit_type", "m_visit_type",
     "k_care_site", "i_exclude"]

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

    discharge_disposition_mapper = CodeMapperDictClass(discharge_disposition_dict, "discharge_disposition_display", "m_discharge_to")

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
                       ("admission_source_display", admit_source_mapper, {"m_admitting_source": "m_admitting_source"})
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
                               (("end_dt_tm", "begin_dt_tm"), FilterHasKeyValueMapper(["end_dt_tm", "begin_dt_tm"], empty_value="0"),
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

    #Claim IDs
    map_claim_id_encounter = os.path.join(input_csv_directory, "Map_Between_Claim_Id_Encounter_Id.csv")
    map_claim_id_encounter_json = create_json_map_from_csv_file(map_claim_id_encounter, "claim_uid", "encounter_id")
    claim_id_encounter_id_mapper = CoderMapperJSONClass(map_claim_id_encounter_json, "claim_id")

    encounter_id_claim_id_mapper = CascadeMapper(FilterHasKeyValueMapper(["encounter_id"]),
                                                 ChainMapper(FilterHasKeyValueMapper(["claim_id"]), claim_id_encounter_id_mapper))
    """
        condition_rules_dx = [(":row_id", "condition_occurrence_id"),
                       ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                       (("encounter_id", "claim_id"),
                        encounter_id_claim_id_mapper,
                        {"visit_occurrence_id": "visit_occurrence_id"}),
                       (("condition_raw_code", "condition_coding_system_id"),
                        ICDMapper,
                        {"CONCEPT_ID": "condition_source_concept_id", "MAPPED_CONCEPT_ID": "condition_concept_id"}),
                       ("condition_raw_code", "condition_source_value"),
                       ("rank_type", condition_type_concept_mapper, {"CONCEPT_ID": "condition_type_concept_id"}),
                       ("effective_dt_tm", SplitDateTimeWithTZ(), {"date": "condition_start_date"})]
    
    """

    ["s_person_id", "s_encounter_id", "s_start_condition_datetime", "s_end_condition_datetime",
     "s_condition_code", "m_condition_code_oid", "s_sequence_id", "m_rank", "s_condition_type",
     "s_present_on_admission_indicator"]

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
    condition_mapper_obj = generate_mapper_obj(ph_f_condition_csv, PHFConditionObject(), source_condition_csv, SourceConditionObject(),
                                               condition_rules, output_class_obj, in_out_map_obj)

    condition_mapper_obj.run()

    """
    [
                                (("procedure_code", "procedure_coding_system_id"), ProcedureCodeMapper,
                                 {"CONCEPT_ID": "procedure_source_concept_id",
                                  "MAPPED_CONCEPT_ID": "procedure_concept_id"},
                                 ),
                                (":row_id", row_map_offset("procedure_occurrence_id", procedure_id_start),
                                  {"procedure_occurrence_id": "procedure_occurrence_id"}),
                                 ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                 (("encounter_id", "claim_id"), encounter_id_mapper,
                                  {"visit_occurrence_id": "visit_occurrence_id"}),
                                 ("service_start_dt_tm", SplitDateTimeWithTZ(),
                                  {"date": "procedure_date"}),
                                 ("procedure_code", "procedure_source_value"),
                                 ("rank_type", procedure_type_map, {"CONCEPT_ID": "procedure_type_concept_id"})
                                 ]
    """

    ["s_person_id", "s_encounter_id", "s_start_procedure_datetime", "s_end_procedure_datetime",
     "s_procedure_code", "m_procedure_code_oid", "s_sequence_id", "s_rank"]

    procedure_rules = [("empi_id", "s_person_id"),
                       (("encounter_id", "claim_id"), encounter_id_claim_id_mapper, {"encounter_id": "s_encounter_id"}),
                       ("procedure_code", "s_procedure_code"),
                       ("procedure_coding_system_id", "m_procedure_code_oid"),
                       ("service_start_dt_tm", "s_start_procedure_datetime"),
                       ("rank_type", "s_rank")]

    ph_f_procedure_csv = os.path.join(input_csv_directory, "PH_F_Procedure.csv")
    source_procedure_csv = os.path.join(output_csv_directory, "source_procedure.csv")

    procedure_mapper_obj = generate_mapper_obj(ph_f_procedure_csv, PHFProcedureObject(), source_procedure_csv, SourceProcedureObject(),
                                               procedure_rules, output_class_obj, in_out_map_obj)

    procedure_mapper_obj.run()

    """
        medication_rules = [(":row_id", row_map_offset("drug_exposure_id", row_offset),
                                      {"drug_exposure_id": "drug_exposure_id"}),
                        ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                        ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                        ("drug_raw_code", "drug_source_value"),
                        ("route_display", "route_source_value"),
                        ("status_display", "stop_reason"), #TODO: LeftMapperString(20)
                        ("route_display", route_mapper, {"mapped_value": "route_concept_id"}),
                        ("dose_quantity", "dose_source_value"),
                        ("start_dt_tm", SplitDateTimeWithTZ(), {"date": "drug_exposure_start_date"}),
                        ("stop_dt_tm", SplitDateTimeWithTZ(), {"date": "drug_exposure_end_date"}),
                        ("dose_quantity", "quantity"),
                        ("dose_unit_display", "dose_unit_source_value"),
                        ("dose_unit_display", snomed_mapper, {"CONCEPT_ID": "dose_unit_concept_id"}),
                        (("drug_raw_coding_system_id", "drug_raw_code", "drug_primary_display"), drug_source_concept_mapper,
                         {"CONCEPT_ID": "drug_source_concept_id"}),
                        (("drug_raw_coding_system_id", "drug_raw_code", "drug_primary_display"), rxnorm_concept_mapper,
                         {"CONCEPT_ID": "drug_concept_id"}), # TODO: Make sure map maps to standard concept
                        ("intended_dispenser", drug_type_mapper, {"CONCEPT_ID": "drug_type_concept_id"})]
    """

    ["s_person_id", "s_encounter_id", "s_drug_code", "m_drug_code_oid", "s_drug_text",
     "s_start_medication_datetime", "s_end_medication_datetime",
     "s_route", "s_quantity", "s_dose", "s_dose_unit", "s_status", "s_drug_type", "s_intended_dispenser"]

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
                        ("status_display", "s_status")]

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