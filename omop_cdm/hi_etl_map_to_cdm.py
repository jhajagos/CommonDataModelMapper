"""
Mapping data extracted from HealtheIntent Data warehouse into the OMOP CDM
"""

from omop_cdm_functions import *
from omop_cdm_classes import *
from hi_classes import *
import os
from mapping_classes import *
import logging
logging.basicConfig(level=logging.INFO)

# Input and output routers


def person_router_obj(input_dict):
    """Route a person"""
    return PersonObject()


def death_router_obj(input_dict):
    """Determines if a row_dict codes a death"""
    if input_dict["deceased"] == "true":
        return DeathObject()
    else:
        return NoOutputClass()


def visit_router_obj(input_dict):
    return VisitOccurrenceObject()


def drug_exposure_router_obj(input_dict):
    """Route mapping of drug_exposure"""
    if input_dict["status_primary_display"] not in  ("Deleted", "Cancelled"):
        return DrugExposureObject()
    else:
        return NoOutputClass()


def measurement_router_obj(input_dict):
    """Determine if the result contains a LOINC code"""
    if "-" in input_dict["result_code"]:
        return MeasurementObject()
    else:
        return NoOutputClass()


# Functions for determining coding system
def condition_coding_system(coding_system_oid):
    """Determine from the OID the coding system for conditions"""
    if coding_system_oid == "2.16.840.1.113883.6.90":
        return "ICD10CM"
    elif coding_system_oid == "2.16.840.1.113883.6.103":
        return "ICD9CM"
    else:
        return False


def drug_code_coding_system(input_dict, field="drug_raw_coding_system_id"):
    """Determine from the OID the coding system for medication"""
    coding_system_oid = input_dict[field]

    if coding_system_oid == "2.16.840.1.113883.6.311":
        return "Multum Main Drug Code (MMDC)"
    elif coding_system_oid == "2.16.840.1.113883.6.312": # | MMSL - Multum drug synonym MMDC | BN - Fully specified drug brand name that can not be prescribed
        return "Multum drug synonym"
    elif coding_system_oid == "2.16.840.1.113883.6.314": # MMSL - GN - d04373 -- Generic drug name
        return "Multum drug identifier (dNUM)"
    elif coding_system_oid == "2.16.840.1.113883.6.88":
        return "RxNorm (RXCUI)"
    else:
        return False


def procedure_coding_system(input_dict, field="procedure_coding_system_id"):
    """Determine from the OID in procedure coding system"""
    coding_system_oid = input_dict[field]

    if coding_system_oid == '2.16.840.1.113883.6.104':
        return 'ICD9 Procedure Codes'
    elif coding_system_oid == '2.16.840.1.113883.6.12':
        return 'CPT Codes'
    elif coding_system_oid == '2.16.840.1.113883.6.14':
        return 'HCFA Procedure Codes'
    elif coding_system_oid == '2.16.840.1.113883.6.4':
        return 'ICD10 Procedure Codes'
    elif coding_system_oid == '2.16.840.1.113883.6.96':
        return 'SNOMED'
    else:
        return False


def main(input_csv_directory, output_csv_directory, json_map_directory):

    # TODO: Add Provider

    # TODO: Add Patient Location

    # Person input / output

    gender_json = os.path.join(json_map_directory, "CONCEPT_NAME_Gender.json")
    gender_json_mapper = CoderMapperJSONClass(gender_json)
    upper_case_mapper = TransformMapper(lambda x: x.upper())
    gender_mapper = ChainMapper(upper_case_mapper, gender_json_mapper)

    input_person_csv = os.path.join(input_csv_directory, "PH_D_Person.csv")
    hi_person_csv_obj = InputClassCSVRealization(input_person_csv, PHDPersonObject())

    output_person_csv = os.path.join(output_csv_directory, "person_cdm.csv")
    cdm_person_csv_obj = OutputClassCSVRealization(output_person_csv, PersonObject())

    # time_of_birth, race_concept_id, ethnicity_concept_id, location_id, provider_id, care_site_id

    # Person input mapper
    # TODO: Replace :row_id with starting seed that increments

    patient_rules = [(":row_id", "person_id"), ("empi_id", "person_source_value"),
                     ("birth_date", DateSplit(),
                        {"year": "year_of_birth", "month": "month_of_birth", "day": "day_of_birth"}),
                     ("gender_display", "gender_source_value"),
                     ("gender_display", gender_mapper, {"CONCEPT_ID": "gender_concept_id"})
                     ]

    patient_mapper_rules_class = build_input_output_mapper(patient_rules)

    in_out_map_obj = InputOutputMapperDirectory()
    in_out_map_obj.register(PHDPersonObject(), PersonObject(), patient_mapper_rules_class)

    output_directory_obj = OutputClassDirectory()
    output_directory_obj.register(PersonObject(), cdm_person_csv_obj)

    person_runner_obj = RunMapperAgainstSingleInputRealization(hi_person_csv_obj, in_out_map_obj, output_directory_obj,
                                                            person_router_obj)
    person_runner_obj.run()

    person_json_file_name = create_json_map_from_csv_file(output_person_csv, "person_source_value", "person_id")
    empi_id_mapper = CoderMapperJSONClass(person_json_file_name)

    # Map to CDM Death

    death_concept_mapper = ChainMapper(ReplacementMapper({"true": 'EHR record patient status "Deceased"'}),
                                                         CoderMapperJSONClass(os.path.join(json_map_directory,
                                                                                           "CONCEPT_NAME_Death_Type.json")))

    hi_person_death_csv_obj = InputClassCSVRealization(input_person_csv, PHDPersonObject())

    death_rules = [("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                   ("deceased", death_concept_mapper, {"CONCEPT_ID":  "death_type_concept_id"}),
                   ("deceased_dt_tm", SplitDateTimeWithTZ(), {"date": "death_date"})]

    output_death_csv = os.path.join(output_csv_directory, "death_cdm.csv")
    cdm_death_csv_obj = OutputClassCSVRealization(output_death_csv, DeathObject())

    death_mapper_rules_class = build_input_output_mapper(death_rules)

    in_out_map_obj.register(PHDPersonObject(), DeathObject(), death_mapper_rules_class)
    output_directory_obj.register(DeathObject(), cdm_death_csv_obj)

    death_runner_obj = RunMapperAgainstSingleInputRealization(hi_person_death_csv_obj, in_out_map_obj, output_directory_obj,
                                                              death_router_obj)

    death_runner_obj.run()

    # Visit Occurrence

    visit_concept_json = os.path.join(json_map_directory, "CONCEPT_NAME_Visit.json")

    #TODO: Add Outpatient mapping
    visit_concept_mapper = ChainMapper(
        ReplacementMapper({"Inpatient": "Inpatient Visit", "Emergency": "Emergency Room Visit"}),
        CoderMapperJSONClass(visit_concept_json))

    visit_concept_type_json = os.path.join(json_map_directory, "CONCEPT_NAME_Visit_Type.json")
    visit_concept_type_mapper = ChainMapper(ConstantMapper({"visit_concept_name": "Visit derived from EHR record"}),
                                            CoderMapperJSONClass(visit_concept_type_json))

    input_encounter_csv = os.path.join(input_csv_directory, "PH_F_Encounter.csv")
    hi_encounter_csv_obj = InputClassCSVRealization(input_encounter_csv, PHFEncounterObject())

    output_visit_occurrence_csv = os.path.join(output_csv_directory, "visit_occurrence_cdm.csv")
    cdm_visit_occurrence_csv_obj = OutputClassCSVRealization(output_visit_occurrence_csv, VisitOccurrenceObject())

    visit_rules = [("encounter_id", "visit_source_value"),
                   ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                   (":row_id", "visit_occurrence_id"),
                   ("classification_primary_display", visit_concept_mapper, {"CONCEPT_ID": "visit_concept_id"}),
                   (":row_id", visit_concept_type_mapper, {"CONCEPT_ID": "visit_type_concept_id"}),
                   ("service_dt_tm", SplitDateTimeWithTZ(), {"date": "visit_start_date", "time": "visit_start_time"}),
                   ("discharge_dt_tm", SplitDateTimeWithTZ(), {"date": "visit_end_date", "time": "visit_end_time"})
                  ]

    # TODO: Add care site id

    visit_rules_class = build_input_output_mapper(visit_rules)

    in_out_map_obj.register(PHFEncounterObject(), VisitOccurrenceObject(), visit_rules_class)
    output_directory_obj.register(VisitOccurrenceObject(), cdm_visit_occurrence_csv_obj)

    visit_runner_obj = RunMapperAgainstSingleInputRealization(hi_encounter_csv_obj, in_out_map_obj,
                                                              output_directory_obj,
                                                              visit_router_obj)

    # Need to check why the synpuf to CDM visit_concept_type and visit_type might be mapped wrong

    visit_runner_obj.run()

    encounter_json_file_name = create_json_map_from_csv_file(output_visit_occurrence_csv, "visit_source_value", "visit_occurrence_id")
    encounter_id_mapper = CoderMapperJSONClass(encounter_json_file_name)

    # measurement

    # ["measurement_id", "person_id", "measurement_concept_id", "measurement_date", "measurement_time", "measurement_type_concept_id",
    # "operator_concept_id", "value_as_number", "value_as_concept_id", "unit_concept_id", "range_low", "range_high", "provider_id",
    # "visit_occurrence_id", "measurement_source_value", "measurement_source_concept_id", "unit_source_value", "value_source_value"]

    input_result_csv = os.path.join(input_csv_directory, "PH_F_Result.csv")
    hi_result_csv_obj = InputClassCSVRealization(input_result_csv, PHFResultObject())

    output_measurement_csv = os.path.join(output_csv_directory, "measurement_cdm_encounter.csv")
    cdm_measurement_csv_obj = OutputClassCSVRealization(output_measurement_csv, MeasurementObject())

    loinc_json = os.path.join(json_map_directory, "LOINC_with_parent.json")
    loinc_mapper = CoderMapperJSONClass(loinc_json)

    snomed_json = os.path.join(json_map_directory, "CONCEPT_NAME_SNOMED.json")  # We don't need the entire SNOMED
    snomed_mapper = CoderMapperJSONClass(snomed_json)

    unit_measurement_mapper = ChainMapper(ReplacementMapper({"s": "__s__"}), snomed_mapper)

    # TODO Add Snomed Mapper
    # TODO: mapping for "measurement_type_concept_id"
    # "Derived value" "From physical examination"  "Lab result"  "Pathology finding"   "Patient reported value"   "Test ordered through EHR"
    # "CONCEPT_CLASS_ID": "Lab Test"

    # TODO: Add value_as_concept_id
    # TODO: Will need to map standard concepts to values

    numeric_coded_mapper = FilterHasKeyValueMapper(["numeric_value", "norm_codified_value_primary_display"])

    measurement_rules = [(":row_id", "measurement_id"),
                         ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                         ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                         ("service_date", SplitDateTimeWithTZ(), {"date": "measurement_date", "time": "measurement_time"}),
                         ("result_code", "measurement_source_value"),  # TODO: Add logic norm_codified_value_display
                         ("result_code", loinc_mapper, {"CONCEPT_ID": "measurement_source_concept_id", "MAPPED_CONCEPT_ID": "measurement_concept_id"}),
                         ("numeric_value", FloatMapper(), "value_as_number"),
                         ("norm_codified_value_primary_display", snomed_mapper, {"CONCEPT_ID": "value_as_concept_id"}),
                         ("norm_unit_of_measure_primary_display", "unit_source_value"),
                         ("norm_unit_of_measure_primary_display", unit_measurement_mapper, {"CONCEPT_ID": "unit_concept_id"}),
                         (("numeric_value", "norm_codified_value_primary_display"), numeric_coded_mapper,
                          {"numeric_value": "value_source_value", "norm_codified_value_primary_display": "value_source_value"}),
                         ("norm_ref_range_low", FloatMapper(), "range_low"),  # TODO: Some values contain non-numeric elements
                         ("norm_ref_range_high", FloatMapper(), "range_high")]

    measurement_rules_class = build_input_output_mapper(measurement_rules)

    in_out_map_obj.register(PHFResultObject(), MeasurementObject(), measurement_rules_class)
    output_directory_obj.register(MeasurementObject(), cdm_measurement_csv_obj)

    measurement_runner_obj = RunMapperAgainstSingleInputRealization(hi_result_csv_obj, in_out_map_obj,
                                                              output_directory_obj,
                                                              measurement_router_obj)
    measurement_runner_obj.run()

    # Conditions
    # PH_F_Condition

    # condition_id: Admitting, Billing Diagnosis, Discharge, Final, Other, Reason For Visit Working

    # "CONCEPT_NAME_Condition_Type.json

    input_condition_csv = os.path.join(input_csv_directory, "PH_F_Condition.csv")
    hi_condition_csv_obj = InputClassCSVRealization(input_condition_csv, PHFConditionObject())

    output_condition_csv = os.path.join(output_csv_directory, "condition_occurrence_dx_cdm_encounter.csv")
    cdm_condition_csv_obj = OutputClassCSVRealization(output_condition_csv, ConditionOccurrenceObject())

    icd9cm_json = os.path.join(json_map_directory, "ICD9CM_with_parent.json")
    icd10cm_json = os.path.join(json_map_directory, "ICD10CM_with_parent.json")

    def case_mapper_icd9_icd10(input_dict, field="condition_coding_system_id"):
        """Map ICD9 and ICD10 to the CDM vocabularies"""
        coding_system_oid = input_dict[field]
        coding_version = condition_coding_system(coding_system_oid)

        if coding_version == "ICD9CM":
            return 0
        else:
            return 1

    #TODO: condition_type_concept_id
    # PNED codes are not getting mapped
    #   Reason for visit diagnosis     2xxx
    #       Complaint of               1xxx
    #       Confirmed                  1xxx
    #
    # 'Patient Self-Reported Condition'
    # 'EHR Episode Entry'
    # These codes do not appear to map to a known external vocabulary

    #Final diagnosis(discharge) 3xxx
    #Complaint of 1x
    #Confirmed 3xxx
    #Possible 1x
    #Probable x
    # 'Secondary Condition'

    ICDMapper = CaseMapper(case_mapper_icd9_icd10, CoderMapperJSONClass(icd9cm_json, "condition_raw_code"), CoderMapperJSONClass(icd10cm_json, "condition_raw_code"))
    condition_rules_dx_encounter = [(":row_id", "condition_occurrence_id"),
                       ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                       ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                       (("condition_raw_code", "condition_coding_system_id"), ICDMapper, {"CONCEPT_ID": "condition_source_concept_id", "MAPPED_CONCEPT_ID": "condition_concept_id"}),
                       ("condition_raw_code", "condition_source_value"),
                       ("effective_dt_tm", SplitDateTimeWithTZ(), {"date": "condition_start_date"})
                      ]

    condition_rules_dx_encounter_class = build_input_output_mapper(condition_rules_dx_encounter)

    # ICD9 and ICD10 conditions which map to measurements according to the CDM Vocabulary

    in_out_map_obj.register(PHFConditionObject(), ConditionOccurrenceObject(), condition_rules_dx_encounter_class)
    output_directory_obj.register(ConditionOccurrenceObject(), cdm_condition_csv_obj)

    measurement_rules_dx_encounter = [(":row_id", "measurement_id"),
                                      ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                      ("encounter_id", encounter_id_mapper,
                                       {"visit_occurrence_id": "visit_occurrence_id"}),
                                      ("effective_dt_tm", SplitDateTimeWithTZ(),
                                        {"date": "measurement_date", "time": "measurement_time"}),
                                      ("condition_code", "measurement_source_value"),
                                      (("condition_raw_code", "condition_coding_system_id"), ICDMapper, {"CONCEPT_ID": "measurement_source_concept_id",
                                                                       "MAPPED_CONCEPT_ID": "measurement_concept_id"})
                                      ]

    measurement_rules_dx_encounter_class = build_input_output_mapper(measurement_rules_dx_encounter)
    in_out_map_obj.register(PHFConditionObject(), MeasurementObject(), measurement_rules_dx_encounter_class)

    # The mapped ICD9 to measurements get mapped to a separate code
    output_measurement_dx_encounter_csv = os.path.join(output_csv_directory, "measurement_dx_encounter_cdm.csv")
    output_measurement_dx_encounter_csv_obj = OutputClassCSVRealization(output_measurement_dx_encounter_csv,
                                                                        MeasurementObject())

    output_directory_obj.register(MeasurementObject(), output_measurement_dx_encounter_csv_obj)

    # ICD9 and ICD10 codes which map to observations according to the CDM Vocabulary
    observation_rules_dx_encounter = [(":row_id","observation_id"),
                                      ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                      ("encounter_id", encounter_id_mapper,
                                       {"visit_occurrence_id": "visit_occurrence_id"}),
                                      ("effective_dt_tm", SplitDateTimeWithTZ(),
                                       {"date": "observation_date", "time": "observation_time"}),
                                      ("condition_code", "observation_source_value"),
                                      (("condition_raw_code", "condition_coding_system_id"), ICDMapper,
                                      {"CONCEPT_ID": "observation_source_concept_id",
                                       "MAPPED_CONCEPT_ID": "observation_concept_id"})
                                      ]

    observation_rules_dx_encounter_class = build_input_output_mapper(observation_rules_dx_encounter)

    output_observation_dx_encounter_csv = os.path.join(output_csv_directory, "observation_dx_encounter_cdm.csv")
    output_observation_dx_encounter_csv_obj = OutputClassCSVRealization(output_observation_dx_encounter_csv,
                                                                        ObservationObject())

    output_directory_obj.register(ObservationObject(), output_observation_dx_encounter_csv_obj)
    in_out_map_obj.register(PHFConditionObject(), ObservationObject(), observation_rules_dx_encounter_class)

    # ICD9 and ICD10 codes which map to procedures according to the CDM Vocabulary

    # TODO: Map procedure_type_concept_id
    procedure_rules_dx_encounter = [(":row_id", "procedure_id"),
                                      ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                      ("encounter_id", encounter_id_mapper,
                                       {"visit_occurrence_id": "visit_occurrence_id"}),
                                      ("effective_dt_tm", SplitDateTimeWithTZ(),
                                       {"date": "procedure_date"}),
                                      ("condition_code", "procedure_source_value"),
                                      (("condition_raw_code", "condition_coding_system_id"), ICDMapper,
                                       {"CONCEPT_ID": "procedure_source_concept_id",
                                        "MAPPED_CONCEPT_ID": "procedure_concept_id"})
                                      ]

    procedure_rules_dx_encounter_class = build_input_output_mapper(procedure_rules_dx_encounter)

    output_procedure_dx_encounter_csv = os.path.join(output_csv_directory, "procedure_dx_encounter_cdm.csv")
    output_procedure_dx_encounter_csv_obj = OutputClassCSVRealization(output_procedure_dx_encounter_csv,
                                                                        ProcedureOccurrenceObject())

    output_directory_obj.register(ProcedureOccurrenceObject(), output_procedure_dx_encounter_csv_obj)
    in_out_map_obj.register(PHFConditionObject(), ProcedureOccurrenceObject(), procedure_rules_dx_encounter_class)

    def condition_router_obj(input_dict):
        """ICD9 / ICD10 CM contain codes which could either be a procedure, observation, or measurement"""
        coding_system_oid = input_dict["condition_coding_system_id"]

        if coding_system_oid:
            result_dict = ICDMapper.map(input_dict)
            if result_dict != {}:
                if result_dict["DOMAIN_ID"] == "Condition":
                    return ConditionOccurrenceObject()
                elif result_dict["DOMAIN_ID"] == "Observation":
                    return ObservationObject()
                elif result_dict["DOMAIN_ID"] == "Procedure":
                    return ProcedureOccurrenceObject()
                elif result_dict["DOMAIN_ID"] == "Measurement":
                    return MeasurementObject()
                else:
                    return NoOutputClass()

            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    condition_runner_obj = RunMapperAgainstSingleInputRealization(hi_condition_csv_obj, in_out_map_obj,
                                                                    output_directory_obj,
                                                                    condition_router_obj)
    condition_runner_obj.run()

    # Maps the DXs linked by the claims

    # procedure

    # 2.16.840.1.113883.6.104 -- ICD9 Procedure Codes
    # 2.16.840.1.113883.6.12  -- CPT Codes
    # 2.16.840.1.113883.6.14  -- HCFA Procedure Codes
    # 2.16.840.1.113883.6.4 -- ICD10 Procedure Codes
    # 2.16.840.1.113883.6.96 -- SNOMED

    def case_mapper_procedures(input_dict, field="procedure_coding_system_id"):

        proc_code_oid = procedure_coding_system(input_dict, field=field)

        if proc_code_oid == "ICD9 Procedure Codes":
            return 0
        elif proc_code_oid == "ICD10 Procedure Codes":
            return 1
        elif proc_code_oid == "CPT Codes":
            return 2

    icd9proc_json = os.path.join(json_map_directory, "ICD9Proc_with_parent.json")
    icd10proc_json = os.path.join(json_map_directory, "ICD10PCS_with_parent.json")
    cpt_json = os.path.join(json_map_directory, "CPT4_with_parent.json")

    #TODO: Add SNOMED and HCPCS Codes to the Mapping

    ProcedureCodeMapper = CaseMapper(case_mapper_procedures,
                                     CoderMapperJSONClass(icd9proc_json, "procedure_raw_code"),
                                     CoderMapperJSONClass(icd10proc_json, "procedure_raw_code"),
                                     CoderMapperJSONClass(cpt_json, "procedure_raw_code")
                                     )

    input_proc_csv = os.path.join(input_csv_directory, "PH_F_Procedure.csv")
    hi_procedure_csv_obj = InputClassCSVRealization(input_proc_csv, PHFProcedureObject())

    procedure_rules_encounter = [(":row_id", "procedure_id"),
                                    ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                    ("encounter_id", encounter_id_mapper,
                                     {"visit_occurrence_id": "visit_occurrence_id"}),
                                    ("service_start_dt_tm", SplitDateTimeWithTZ(),
                                     {"date": "procedure_date"}),
                                    ("procedure_raw_code", "procedure_source_value"),
                                    (("procedure_raw_code", "procedure_coding_system_id"), ProcedureCodeMapper,
                                     {"CONCEPT_ID": "procedure_source_concept_id",
                                      "MAPPED_CONCEPT_ID": "procedure_concept_id"})]

    procedure_rules_encounter_class = build_input_output_mapper(procedure_rules_encounter)

    output_procedure_encounter_csv = os.path.join(output_csv_directory, "procedure_encounter_cdm.csv")
    output_procedure_encounter_csv_obj = OutputClassCSVRealization(output_procedure_encounter_csv,
                                                                   ProcedureOccurrenceObject())

    output_directory_obj.register(ProcedureOccurrenceObject(), output_procedure_encounter_csv_obj)
    in_out_map_obj.register(PHFProcedureObject(), ProcedureOccurrenceObject(), procedure_rules_encounter_class)

    def procedure_router_obj(input_dict):
        if procedure_coding_system(input_dict) in ("ICD9 Procedure Codes", "ICD10 Procedure Codes", "CPT Codes"):
            return ProcedureOccurrenceObject()
        else:
            return NoOutputClass()

    procedure_runner_obj = RunMapperAgainstSingleInputRealization(hi_procedure_csv_obj, in_out_map_obj,
                                                                  output_directory_obj,
                                                                  procedure_router_obj)

    procedure_runner_obj.run()

    # Drug_Exposure

    input_med_csv = os.path.join(input_csv_directory, "PH_F_Medication.csv")
    hi_medication_csv_obj = InputClassCSVRealization(input_med_csv, PHFMedicationObject())

    output_drug_exposure_csv = os.path.join(output_csv_directory, "drug_exposure_cdm.csv")
    cdm_drug_exposure_csv_obj = OutputClassCSVRealization(output_drug_exposure_csv, DrugExposureObject())

    multum_bn_json = os.path.join(json_map_directory, "RxNorm_MMSL_BN.json")
    multum_gn_json = os.path.join(json_map_directory, "RxNorm_MMSL_GN.json")
    multum_bd_json = os.path.join(json_map_directory, "RxNorm_MMSL_BD.json")

    rxcui_mapper_json = os.path.join(json_map_directory, "CONCEPT_CODE_RxNorm.json")

    def case_mapper_drug_code(input_dict, field="drug_raw_coding_system_id"):
        drug_coding_system_name = drug_code_coding_system(input_dict, field=field)

        if drug_coding_system_name == "Multum drug synonym":
            return 0
        elif drug_coding_system_name == "Multum drug identifier (dNUM)":
            return 1
        elif drug_coding_system_name == "Multum Main Drug Code (MMDC)":
            return 2
        elif drug_coding_system_name == "RxNorm (RxCUI)":
            return 3
        else:
            return False

    DrugCodeMapper = ChainMapper(CaseMapper(case_mapper_drug_code,
                                            CoderMapperJSONClass(multum_bn_json, "drug_raw_code"),
                                            CoderMapperJSONClass(multum_gn_json, "drug_raw_code"),
                                            CoderMapperJSONClass(multum_bd_json, "drug_raw_code"),
                                            KeyTranslator({"drug_raw_code": "RXCUI"})),
                                            CoderMapperJSONClass(rxcui_mapper_json, "RXCUI"))


    rxnorm_name_json = os.path.join(json_map_directory, "CONCEPT_NAME_RxNorm.json")

    rxnorm_name_mapper = CoderMapperJSONClass(rxnorm_name_json)

    def string_to_cap_first_letter(raw_string):
        if len(raw_string):
            return raw_string[0].upper() + raw_string[1:].lower()
        else:
            return raw_string


    rxnorm_name_mapper_chained = ChainMapper(TransformMapper(string_to_cap_first_letter), rxnorm_name_mapper)

    drug_mapper = CascadeMapper(DrugCodeMapper, rxnorm_name_mapper_chained)


    # TODO: Map dose_unit_source_value -> drug_unit_concept_id
    # TODO: Map route_source_value -> route_source_value



    drug_type_json = os.path.join(json_map_directory, "CONCEPT_NAME_Drug_Type.json")
    drug_type_code_mapper = CoderMapperJSONClass(drug_type_json)

    drug_type_mapper = ChainMapper(ReplacementMapper({"HOSPITAL_PHARMACY": "Inpatient administration",
                           "INPATIENT_FLOOR_STOCK": "Inpatient administration",
                           "RETAIL_PHARMACY": "Prescription dispensed in pharmacy",
                           "UNKNOWN": "Prescription dispensed in pharmacy"}), drug_type_code_mapper)

    # TODO: Rework this mapper
    #Source: http://forums.ohdsi.org/t/route-standard-concepts-not-standard-anymore/1300/7
    routes_to_concept_id_dict = {
        "Gastroenteral": "4186834",
        "Cutaneous": "4263689",
        "Ocular": "4184451",
        "Intramuscular": "4302612",
        "Buccal": "4181897",
        "Nasal": "4262914",
        "Transdermal": "4262099",
        "Body cavity use": "4222254",
        "Vaginal": "4057765",
        "Intradermal": "4156706",
        "Epidural": "4225555",
        "Auricular": "4023156",
        "Intralesional": "4157758",
        "Intrauterine": "4269621",
        "Intraarticular": "4006860",
        "Intravesical": "4186838",
        "Dental": "4163765",
        "Intraperitoneal": "4243022",
        "Intravitreal": "4302785",
        "Intrapleural": "4156707",
        "Intrabursal": "4163768",
        "Intraosseous": "4213522",
        "Intravenous": "4112421",
        "Rectal": "4115462",
        "Inhaling": "4120036",
        "Oral": "4128794",
        "Subcutaneous": "4139962",
        "Intravaginal": "4136280",
        "Topical": "4231622",
        "Intraocular": "4157760",
        "Intrathecal": "4217202",
        "Urethral": "4233974"
    }

    route_mapper = ChainMapper(ReplacementMapper({"SubCutaneous": "Subcutaneous", "IV Push": "Intravenous",
                                                      "Continuous IV": "Intravenous", "IntraMuscular": "Intramuscular"}),
                                                  CodeMapperDictClass(routes_to_concept_id_dict))


    medication_rules = [(":row_id", "drug_exposure_id"),
                        ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                        ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                        (("drug_raw_code", "drug_primary_display", "drug_raw_coding_system_id"),
                         ConcatenateMapper("|", "drug_primary_display", "drug_raw_code", "drug_raw_coding_system_id"),
                         {"drug_primary_display|drug_raw_code|drug_raw_coding_system_id": "drug_source_value"}),
                        ("route_display", "route_source_value"),
                        ("status_display","stop_reason"),
                        ("route_display", route_mapper, {"mapped_value": "route_concept_id"}),
                        ("dose_quantity", "dose_source_value"),
                        ("start_dt_tm", SplitDateTimeWithTZ(), {"date": "drug_exposure_start_date"}),
                        ("stop_dt_tm", SplitDateTimeWithTZ(), {"date": "drug_exposure_end_date"}),
                        ("dose_quantity", "quantity"),
                        ("dose_unit_display", "dose_unit_source_value"),
                        ("dose_unit_display", snomed_mapper, {"CONCEPT_ID": "dose_unit_concept_id"}),
                        (("drug_raw_coding_system_id", "drug_raw_code", "drug_primary_display"), drug_mapper,
                         {"CONCEPT_ID": "drug_source_concept_id"}),
                        (("drug_raw_coding_system_id", "drug_raw_code", "drug_primary_display"), drug_mapper,
                        {"CONCEPT_ID": "drug_concept_id"}),
                        ("intended_dispenser", drug_type_mapper, {"CONCEPT_ID": "drug_type_concept_id"})
                        ]

    medication_rules_class = build_input_output_mapper(medication_rules)

    in_out_map_obj.register(PHFMedicationObject(), DrugExposureObject(), medication_rules_class)
    output_directory_obj.register(DrugExposureObject(), cdm_drug_exposure_csv_obj)

    drug_exposure_runner_obj = RunMapperAgainstSingleInputRealization(hi_medication_csv_obj, in_out_map_obj,
                                                                      output_directory_obj,
                                                                      drug_exposure_router_obj)

    drug_exposure_runner_obj.run()

    #  DRGs MS-DRGS

    # ["observation_id", "person_id", "observation_concept_id", "observation_date", "observation_time",
    #  "observation_type_concept_id", "value_as_number", "value_as_string", "value_as_concept_id", "qualifier_concept_id",
    #  "unit_concept_id", "provider_id", "visit_occurrence_id", "observation_source_value",
    #  "observation_source_concept_id", "unit_source_value", "qualifier_source_value"]


if __name__ == "__main__":
    with open("hi_config.json", "r") as f:
        config_dict = json.load(f)

    main(config_dict["csv_input_directory"], config_dict["csv_output_directory"], config_dict["json_map_directory"])

    # [u'Carrier claim detail - 10th position',
    #  u'Carrier claim detail - 11th position',
    #  u'Carrier claim detail - 12th position',
    #  u'Carrier claim detail - 13th position',
    #  u'Carrier claim detail - 1st position',
    #  u'Carrier claim detail - 2nd position',
    #  u'Carrier claim detail - 3rd position',
    #  u'Carrier claim detail - 4th position',
    #  u'Carrier claim detail - 5th position',
    #  u'Carrier claim detail - 6th position',
    #  u'Carrier claim detail - 7th position',
    #  u'Carrier claim detail - 8th position',
    #  u'Carrier claim detail - 9th position',
    #  u'Carrier claim header - 1st position',
    #  u'Carrier claim header - 2nd position',
    #  u'Carrier claim header - 3rd position',
    #  u'Carrier claim header - 4th position',
    #  u'Carrier claim header - 5th position',
    #  u'Carrier claim header - 6th position',
    #  u'Carrier claim header - 7th position',
    #  u'Carrier claim header - 8th position',
    #  u'Condition era - 0 days persistence window',
    #  u'Condition era - 30 days persistence window',
    #  u'EHR Chief Complaint',
    #  u'EHR Episode Entry',
    #  u'EHR problem list entry',
    #  u'First Position Condition',
    #  u'Inpatient detail - 10th position',
    #  u'Inpatient detail - 11th position',
    #  u'Inpatient detail - 12th position',
    #  u'Inpatient detail - 13th position',
    #  u'Inpatient detail - 14th position',
    #  u'Inpatient detail - 15th position',
    #  u'Inpatient detail - 16th position',
    #  u'Inpatient detail - 17th position',
    #  u'Inpatient detail - 18th position',
    #  u'Inpatient detail - 19th position',
    #  u'Inpatient detail - 1st position',
    #  u'Inpatient detail - 20th position',
    #  u'Inpatient detail - 2nd position',
    #  u'Inpatient detail - 3rd position',
    #  u'Inpatient detail - 4th position',
    #  u'Inpatient detail - 5th position',
    #  u'Inpatient detail - 6th position',
    #  u'Inpatient detail - 7th position',
    #  u'Inpatient detail - 8th position',
    #  u'Inpatient detail - 9th position',
    #  u'Inpatient detail - primary',
    #  u'Inpatient header - 10th position',
    #  u'Inpatient header - 11th position',
    #  u'Inpatient header - 12th position',
    #  u'Inpatient header - 13th position',
    #  u'Inpatient header - 14th position',
    #  u'Inpatient header - 15th position',
    #  u'Inpatient header - 1st position',
    #  u'Inpatient header - 2nd position',
    #  u'Inpatient header - 3rd position',
    #  u'Inpatient header - 4th position',
    #  u'Inpatient header - 5th position',
    #  u'Inpatient header - 6th position',
    #  u'Inpatient header - 7th position',
    #  u'Inpatient header - 8th position',
    #  u'Inpatient header - 9th position',
    #  u'Inpatient header - primary',
    #  u'Observation recorded from EHR',
    #  u'Outpatient detail - 10th position',
    #  u'Outpatient detail - 11th position',
    #  u'Outpatient detail - 12th position',
    #  u'Outpatient detail - 13th position',
    #  u'Outpatient detail - 14th position',
    #  u'Outpatient detail - 15th position',
    #  u'Outpatient detail - 1st position',
    #  u'Outpatient detail - 2nd position',
    #  u'Outpatient detail - 3rd position',
    #  u'Outpatient detail - 4th position',
    #  u'Outpatient detail - 5th position',
    #  u'Outpatient detail - 6th position',
    #  u'Outpatient detail - 7th position',
    #  u'Outpatient detail - 8th position',
    #  u'Outpatient detail - 9th position',
    #  u'Outpatient header - 10th position',
    #  u'Outpatient header - 11th position',
    #  u'Outpatient header - 12th position',
    #  u'Outpatient header - 13th position',
    #  u'Outpatient header - 14th position',
    #  u'Outpatient header - 15th position',
    #  u'Outpatient header - 1st position',
    #  u'Outpatient header - 2nd position',
    #  u'Outpatient header - 3rd position',
    #  u'Outpatient header - 4th position',
    #  u'Outpatient header - 5th position',
    #  u'Outpatient header - 6th position',
    #  u'Outpatient header - 7th position',
    #  u'Outpatient header - 8th position',
    #  u'Outpatient header - 9th position',
    #  u'Patient Self-Reported Condition',
    #  u'Primary Condition',
    #  u'Referral record',
    #  u'Secondary Condition']