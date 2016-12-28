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


#### INPUT OUTPUT ROUTERS ####
def person_router_obj(input_dict):
    """Route a person"""
    return PersonObject()


def death_router_obj(input_dict):
    """Determines if a row_dict codes a death"""
    if input_dict["deceased"] == "true":
        return DeathObject()
    else:
        return NoOutputClass()


# Functions for determining coding system
def condition_coding_system(coding_system_oid):
    """Determine from the OID the coding system for conditions"""
    if coding_system_oid == "2.16.840.1.113883.6.90":
        return "ICD10CM"
    elif coding_system_oid == "2.16.840.1.113883.6.103":
        return "ICD9CM"
    elif coding_system_oid == '2.16.840.1.113883.6.96':
        return 'SNOMED'
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
    elif coding_system_oid == '2.16.840.1.113883.6.285':
        return 'HCPCS'
    else:
        return False


def register_to_mapper_obj(input_csv_file_name, input_class_obj, output_csv_file_name, output_class_obj,
                            map_rules_list,
                            output_obj, in_out_map_obj):

        input_csv_class_obj = InputClassCSVRealization(input_csv_file_name, input_class_obj)
        output_csv_class_obj = OutputClassCSVRealization(output_csv_file_name, output_class_obj)

        map_rules_obj = build_input_output_mapper(map_rules_list)

        output_obj.register(output_class_obj, output_csv_class_obj)

        in_out_map_obj.register(input_class_obj, output_class_obj, map_rules_obj)


def generate_mapper_obj(input_csv_file_name, input_class_obj, output_csv_file_name, output_class_obj, map_rules_list,
                        output_obj, in_out_map_obj, input_router_func, pre_map_func=None, post_map_func=None):

    input_csv_class_obj = InputClassCSVRealization(input_csv_file_name, input_class_obj)
    output_csv_class_obj = OutputClassCSVRealization(output_csv_file_name, output_class_obj)

    map_rules_obj = build_input_output_mapper(map_rules_list)

    output_obj.register(output_class_obj, output_csv_class_obj)

    in_out_map_obj.register(input_class_obj, output_class_obj, map_rules_obj)

    map_runner_obj = RunMapperAgainstSingleInputRealization(input_csv_class_obj, in_out_map_obj, output_obj,
                                                            input_router_func, pre_map_func, post_map_func)

    return map_runner_obj


#### FUNCTIONS FOR RULES ####
def create_patient_rules(json_map_directory):
    """Generate rules for mapping PH_D_Person mapper"""

    gender_json = os.path.join(json_map_directory, "CONCEPT_NAME_Gender.json")
    gender_json_mapper = CoderMapperJSONClass(gender_json)
    upper_case_mapper = TransformMapper(lambda x: x.upper())
    gender_mapper = ChainMapper(upper_case_mapper, gender_json_mapper)

    # TODO: Replace :row_id with starting seed that increments
    # Required person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id
    patient_rules = [(":row_id", row_map_offset("person_id", 0), {"person_id": "person_id"}),
                     ("empi_id", "person_source_value"),
                     ("birth_date", DateSplit(),
                      {"year": "year_of_birth", "month": "month_of_birth", "day": "day_of_birth"}),
                     ("gender_display", "gender_source_value"),
                     ("gender_display", gender_mapper, {"CONCEPT_ID": "gender_concept_id"}),
                     ("gender_display", gender_mapper, {"CONCEPT_ID": "gender_source_concept_id"})
                     ]

    return patient_rules


def create_death_patient_rules(json_map_directory, empi_id_mapper):
    """Generate rules for mapping death"""

    death_concept_mapper = ChainMapper(ReplacementMapper({"true": 'EHR record patient status "Deceased"'}),
                                       CoderMapperJSONClass(os.path.join(json_map_directory,
                                                                         "CONCEPT_NAME_Death_Type.json")))
    # Required person_id, death_date, death_type_concept_id
    death_rules = [("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                   ("deceased", death_concept_mapper, {"CONCEPT_ID": "death_type_concept_id"}),
                   ("deceased_dt_tm", SplitDateTimeWithTZ(), {"date": "death_date"})]

    return death_rules


def create_observation_period_rules(json_map_directory, empi_id_mapper):
        observation_period_mapper = CoderMapperJSONClass(os.path.join(json_map_directory, "CONCEPT_NAME_Obs_Period_Type.json"))
        observation_period_constant_mapper = ChainMapper(ConstantMapper({"observation_period_type_name": "Period covering healthcare encounters"}), observation_period_mapper)

        observation_period_rules = [(":row_id", "observation_period_id"),
                                    ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                    ("min_service_dt_tm", SplitDateTimeWithTZ(),
                                     {"date": "observation_period_start_date"}),
                                    ("max_service_dt_tm", SplitDateTimeWithTZ(),
                                     {"date": "observation_period_end_date"}),
                                    (":row_id", observation_period_constant_mapper, {"CONCEPT_ID": "period_type_concept_id"})
                                    ]

        return observation_period_rules


def create_visit_rules(json_map_directory, empi_id_mapper):
    """Generate rules for mapping PH_F_Encounter to VisitOccurrence"""

    visit_concept_json = os.path.join(json_map_directory, "CONCEPT_NAME_Visit.json")
    visit_concept_mapper = ChainMapper(
        ReplacementMapper({"Inpatient": "Inpatient Visit", "Emergency": "Emergency Room Visit",
                           "Outpatient": "Outpatient Visit", "Observation": "Emergency Room Visit",
                           "Recurring": "Outpatient Visit", "Preadmit": "Outpatient Visit", "": "Outpatient Visit"
                           }), # Note: there are no observation type
        CoderMapperJSONClass(visit_concept_json))

    visit_concept_type_json = os.path.join(json_map_directory, "CONCEPT_NAME_Visit_Type.json")
    visit_concept_type_mapper = ChainMapper(ConstantMapper({"visit_concept_name": "Visit derived from EHR record"}),
                                            CoderMapperJSONClass(visit_concept_type_json))

    # TODO: Add care site id
    # Required: visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_type_concept_id
    visit_rules = [("encounter_id", "visit_source_value"),
                   ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                   (":row_id", "visit_occurrence_id"),
                   ("classification_display", visit_concept_mapper, {"CONCEPT_ID": "visit_concept_id"}),
                   (":row_id", visit_concept_type_mapper, {"CONCEPT_ID": "visit_type_concept_id"}),
                   ("service_dt_tm", SplitDateTimeWithTZ(), {"date": "visit_start_date", "time": "visit_start_time"}),
                   ("discharge_dt_tm", SplitDateTimeWithTZ(), {"date": "visit_end_date", "time": "visit_end_time"})
                   ]

    return visit_rules


def generate_value_as_concept_mapper(snomed_mapper):
    value_as_concept_mapper = ChainMapper(FilterHasKeyValueMapper(["norm_codified_value_primary_display", "norm_text_value"]),
                                          TransformMapper(capitalize_words_and_normalize_spacing),
                                          ReplacementMapper({"Implant": "implant", "A": "Blood group A", "Ab": "Blood group AB", "O": "Blood group O",
                                                             "B": "Blood group B"
                                                             }),
                                          snomed_mapper)
    return value_as_concept_mapper


def create_measurement_and_observation_rules(json_map_directory, empi_id_mapper, encounter_id_mapper, snomed_mapper, snomed_code_mapper):
    """Generate rules for mapping PH_F_Result to Measurement"""

    unit_measurement_mapper = ChainMapper(ReplacementMapper({"s": "__s__"}), snomed_mapper) # TODO: Switch to UCUM coding

    loinc_json = os.path.join(json_map_directory, "LOINC_with_parent.json")
    loinc_mapper = CoderMapperJSONClass(loinc_json)

    measurement_code_mapper = CascadeMapper(loinc_mapper, snomed_code_mapper, ConstantMapper({"CONCEPT_ID": 0}))

    # TODO: Currently only map "Lab result" add other measurement types "measurement_type_concept_id"
    # TODO: Add value_as_concept_id -  Add more cases where value matches a SNOMED concept

    measurement_type_json = os.path.join(json_map_directory, "CONCEPT_NAME_Meas_Type.json")
    measurement_type_mapper = CoderMapperJSONClass(measurement_type_json)

    value_as_concept_mapper = generate_value_as_concept_mapper(snomed_mapper)

    measurement_type_chained_mapper = CascadeMapper(ChainMapper(loinc_mapper, FilterHasKeyValueMapper(["CONCEPT_CLASS_ID"]),
                                                                 ReplacementMapper({"Lab Test": "Lab result"}),
                                                                 measurement_type_mapper), ConstantMapper({"CONCEPT_ID": 0}))

    # "Derived value" "From physical examination"  "Lab result"  "Pathology finding"   "Patient reported value"   "Test ordered through EHR"
    # "CONCEPT_CLASS_ID": "Lab Test"
    numeric_coded_mapper = FilterHasKeyValueMapper(["norm_numeric_value", "norm_codified_value_primary_display", "norm_text_value", "result_primary_display"])

    measurement_rules = [(":row_id", "measurement_id"),
                         ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                         ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                         ("service_date", SplitDateTimeWithTZ(), {"date": "measurement_date", "time": "measurement_time"}),
                         ("result_code", "measurement_source_value"),
                         ("result_code", measurement_code_mapper,  {"CONCEPT_ID": "measurement_source_concept_id"}),
                         ("result_code", measurement_code_mapper,  {"CONCEPT_ID": "measurement_concept_id"}),
                         ("result_code", measurement_type_chained_mapper, {"CONCEPT_ID": "measurement_type_concept_id"}),
                         ("norm_numeric_value", FloatMapper(), "value_as_number"),
                         (("norm_text_value", "norm_codified_value_primary_display"), value_as_concept_mapper, {"CONCEPT_ID": "value_as_concept_id"}), #norm_codified_value_primary_display",
                         ("norm_unit_of_measure_primary_display", "unit_source_value"),
                         ("norm_unit_of_measure_primary_display", unit_measurement_mapper, {"CONCEPT_ID": "unit_concept_id"}),
                         (("norm_numeric_value", "norm_codified_value_primary_display", "result_primary_display", "norm_text_value"), numeric_coded_mapper,
                          {"norm_numeric_value": "value_source_value",
                           "norm_codified_value_primary_display": "value_source_value",
                           "result_primary_display": "value_source_value",
                           "norm_text_value": "value_source_value"}),
                         ("norm_ref_range_low", FloatMapper(), "range_low"),  # TODO: Some values contain non-numeric elements
                         ("norm_ref_range_high", FloatMapper(), "range_high")]

    #TODO: observation_type_concept_id <- "Observation recorded from EHR"
    measurement_observation_rules = [(":row_id", "observation_id"),
                                     ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                     ("encounter_id", encounter_id_mapper,
                                      {"visit_occurrence_id": "visit_occurrence_id"}),
                                     ("service_date", SplitDateTimeWithTZ(),
                                      {"date": "observation_date", "time": "observation_time"}),
                                     ("result_code", "observation_source_value"),
                                     ("result_code", measurement_code_mapper,
                                      {"CONCEPT_ID": "observation_source_concept_id"}),
                                     ("result_code", measurement_code_mapper,
                                      {"CONCEPT_ID": "observation_concept_id"}),
                                     ("result_code", measurement_type_chained_mapper,
                                      {"CONCEPT_ID": "observation_type_concept_id"}),
                                     ("norm_numeric_value", FloatMapper(), "value_as_number"),
                                     (("norm_text_value", "norm_codified_value_primary_display"),
                                      value_as_concept_mapper, {"CONCEPT_ID": "value_as_concept_id"}),
                                     ("norm_unit_of_measure_primary_display", "unit_source_value"),
                                     ("norm_unit_of_measure_primary_display", unit_measurement_mapper,
                                      {"CONCEPT_ID": "unit_concept_id"}),
                                     (("norm_numeric_value", "norm_codified_value_primary_display", "result_primary_display",
                                       "norm_text_value"), numeric_coded_mapper,
                                      {"norm_numeric_value": "value_source_value",
                                       "norm_codified_value_primary_display": "value_source_value",
                                       "result_primary_display": "value_source_value",
                                       "norm_text_value": "value_source_value"})
                                     ]

    return measurement_rules, measurement_observation_rules


def case_mapper_procedures(input_dict, field="procedure_coding_system_id"):
    proc_code_oid = procedure_coding_system(input_dict, field=field)

    if proc_code_oid == "ICD9 Procedure Codes":
        return 0
    elif proc_code_oid == "ICD10 Procedure Codes":
        return 1
    elif proc_code_oid == "CPT Codes":
        return 2
    elif proc_code_oid == "HCPCS":
        return 3


def create_procedure_rules(json_map_directory, empi_id_mapper, encounter_id_mapper, procedure_id_start):
    # Maps the DXs linked by the claims
    # procedure
    # 2.16.840.1.113883.6.104 -- ICD9 Procedure Codes
    # 2.16.840.1.113883.6.12  -- CPT Codes
    # 2.16.840.1.113883.6.14  -- HCFA Procedure Codes
    # 2.16.840.1.113883.6.4 -- ICD10 Procedure Codes
    # 2.16.840.1.113883.6.96 -- SNOMED

    icd9proc_json = os.path.join(json_map_directory, "ICD9Proc_with_parent.json")
    icd10proc_json = os.path.join(json_map_directory, "ICD10PCS_with_parent.json")
    cpt_json = os.path.join(json_map_directory, "CPT4_with_parent.json")
    hcpcs_json = os.path.join(json_map_directory, "HCPCS_with_parent.json")
    procedure_type_name_json = os.path.join(json_map_directory, "CONCEPT_NAME_Procedure_Type.json")

    procedure_type_map = \
        CascadeMapper(ChainMapper(
                ReplacementMapper({"PRIMARY": "Primary Procedure", "SECONDARY": "Secondary Procedure"}),
                CoderMapperJSONClass(procedure_type_name_json)),
            ConstantMapper({"CONCEPT_ID": 0})
        )

    # TODO: Add SNOMED Codes to the Mapping
    ProcedureCodeMapper = CascadeMapper(CaseMapper(case_mapper_procedures,
                                     CoderMapperJSONClass(icd9proc_json, "procedure_code"),
                                     CoderMapperJSONClass(icd10proc_json, "procedure_code"),
                                     CoderMapperJSONClass(cpt_json, "procedure_code"),
                                     CoderMapperJSONClass(hcpcs_json, "procedure_code"),
                                      ), ConstantMapper({"CONCEPT_ID": 0, "MAPPED_CONCEPT_ID": 0}))

    # Required: procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id
    procedure_rules_encounter = [
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

    return procedure_rules_encounter


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


def generate_rxcui_drug_code_mapper(json_map_directory):

    multum_gn_json = os.path.join(json_map_directory, "RxNorm_MMSL_GN.json")
    multum_json = os.path.join(json_map_directory, "rxnorm_multum.csv.MULDRUG_ID.json")
    multum_drug_json = os.path.join(json_map_directory, "rxnorm_multum_drug.csv.MULDRUG_ID.json")
    multum_drug_mmdc_json = os.path.join(json_map_directory, "rxnorm_multum_mmdc.csv.MULDRUG_ID.json")

    drug_code_mapper = ChainMapper(CaseMapper(case_mapper_drug_code,
                                            CoderMapperJSONClass(multum_json, "drug_raw_code"),
                                            CascadeMapper(
                                                ChainMapper(CoderMapperJSONClass(multum_gn_json, "drug_raw_code"), KeyTranslator({"RXCUI": "RXNORM_ID"})),
                                                CoderMapperJSONClass(multum_drug_json, "drug_raw_code")),
                                            CoderMapperJSONClass(multum_drug_mmdc_json, "drug_raw_code"),
                                            KeyTranslator({"drug_raw_code": "RXNORM_ID"})),
                                            )

    return drug_code_mapper


def generate_drug_name_mapper(json_map_directory):
    rxnorm_name_json = os.path.join(json_map_directory, "CONCEPT_NAME_RxNorm.json")
    rxnorm_name_mapper = CoderMapperJSONClass(rxnorm_name_json, "drug_primary_display")

    def string_to_cap_first_letter(raw_string):
        if len(raw_string):
            return raw_string[0].upper() + raw_string[1:].lower()
        else:
            return raw_string

    rxnorm_name_mapper_chained = CascadeMapper(rxnorm_name_mapper, ChainMapper(TransformMapper(string_to_cap_first_letter), rxnorm_name_mapper))

    return rxnorm_name_mapper_chained


def create_medication_rules(json_map_directory, empi_id_mapper, encounter_id_mapper, snomed_mapper):

    #TODO: Increase mapping coverage of drugs - while likely need manual overrides

    rxnorm_rxcui_mapper = generate_rxcui_drug_code_mapper(json_map_directory)
    rxnorm_name_mapper_chained = generate_drug_name_mapper(json_map_directory)

    # TODO: Increase coverage of "Map dose_unit_source_value -> drug_unit_concept_id"
    # TODO: Increase coverage of "Map route_source_value -> route_source_value"

    drug_type_json = os.path.join(json_map_directory, "CONCEPT_NAME_Drug_Type.json")
    drug_type_code_mapper = CoderMapperJSONClass(drug_type_json)

    rxnorm_code_mapper_json = os.path.join(json_map_directory, "CONCEPT_CODE_RxNorm.json")
    rxnorm_code_concept_mapper = CoderMapperJSONClass(rxnorm_code_mapper_json, "RXNORM_ID")
    drug_source_concept_mapper = ChainMapper(CascadeMapper(ChainMapper(rxnorm_rxcui_mapper, rxnorm_code_concept_mapper), rxnorm_name_mapper_chained))

    rxnorm_bn_in_mapper_json = os.path.join(json_map_directory, "select_n_in__ot___from___select_bn_rxcui.csv.bn_rxcui.json")
    rxnorm_bn_sbdf_mapper_json = os.path.join(json_map_directory, "select_tt_n_sbdf__ott___from___select_bn.csv.bn_rxcui.json")

    rxnorm_bn_in_mapper = CoderMapperJSONClass(rxnorm_bn_in_mapper_json,"RXNORM_ID")
    rxnorm_bn_sbdf_mapper = CoderMapperJSONClass(rxnorm_bn_sbdf_mapper_json, "RXNORM_ID")

    rxnorm_str_bn_in_mapper_json = os.path.join(json_map_directory,
                                            "select_n_in__ot___from___select_bn_rxcui.csv.bn_str.json")
    rxnorm_str_bn_sbdf_mapper_json = os.path.join(json_map_directory,
                                              "select_tt_n_sbdf__ott___from___select_bn.csv.bn_str.json")

    rxnorm_str_bn_in_mapper = CoderMapperJSONClass(rxnorm_str_bn_in_mapper_json)
    rxnorm_str_bn_sbdf_mapper = CoderMapperJSONClass(rxnorm_str_bn_sbdf_mapper_json)

    rxnorm_concept_mapper = CascadeMapper(ChainMapper(CascadeMapper(ChainMapper(rxnorm_rxcui_mapper, ChainMapper(rxnorm_bn_sbdf_mapper, KeyTranslator({"sbdf_rxcui": "RXNORM_ID"}))),
                                          ChainMapper(rxnorm_rxcui_mapper, ChainMapper(rxnorm_bn_in_mapper, KeyTranslator({"in_rxcui": "RXNORM_ID"}))),
                                          rxnorm_rxcui_mapper), rxnorm_code_concept_mapper),
                                          CascadeMapper(ChainMapper(rxnorm_str_bn_sbdf_mapper, rxnorm_name_mapper_chained),
                                                        ChainMapper(rxnorm_str_bn_in_mapper, rxnorm_name_mapper_chained),
                                          rxnorm_name_mapper_chained))

    drug_type_mapper = ChainMapper(ReplacementMapper({"HOSPITAL_PHARMACY": "Inpatient administration",
                                                      "INPATIENT_FLOOR_STOCK": "Inpatient administration",
                                                      "RETAIL_PHARMACY": "Prescription dispensed in pharmacy",
                                                      "UNKNOWN": "Prescription dispensed in pharmacy",
                                                      "_NOT_VALUED": "Prescription written",
                                                      "OFFICE": "Physician administered drug (identified from EHR observation)"
                                                      },
                                                     ),
                                   drug_type_code_mapper)


    # TODO: Rework this mapper not to be static code
    # Source: http://forums.ohdsi.org/t/route-standard-concepts-not-standard-anymore/1300/7
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

    # Required # drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_type_concept_id
    medication_rules = [(":row_id", "drug_exposure_id"),
                        ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                        ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                        (("drug_raw_code", "drug_primary_display", "drug_raw_coding_system_id"),
                         ConcatenateMapper("|", "drug_primary_display", "drug_raw_code", "drug_raw_coding_system_id"),
                         {"drug_primary_display|drug_raw_code|drug_raw_coding_system_id": "drug_source_value"}),
                        ("route_display", "route_source_value"),
                        ("status_display", "stop_reason"),
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
                        ("intended_dispenser", drug_type_mapper, {"CONCEPT_ID": "drug_type_concept_id"})
                        ]

    return medication_rules


def main(input_csv_directory, output_csv_directory, json_map_directory):
    # TODO: Add Provider
    # TODO: Add Patient Location
    # TODO: Handle End Dates

    output_class_obj = OutputClassDirectory()

    in_out_map_obj = InputOutputMapperDirectory()
    output_directory_obj = OutputClassDirectory()

    #### Person ####
    patient_rules = create_patient_rules(json_map_directory)

    input_person_csv = os.path.join(input_csv_directory, "PH_D_Person.csv")
    output_person_csv = os.path.join(output_csv_directory, "person_cdm.csv")

    #TODO: Add Race and Ethnicity Mapping
    def post_map_person_func(map_dict):
        map_dict["ethnicity_concept_id"] = 0
        map_dict["race_concept_id"] = 0
        return map_dict

    person_runner_obj = generate_mapper_obj(input_person_csv, PHDPersonObject(), output_person_csv, PersonObject(), patient_rules,
                        output_class_obj, in_out_map_obj, person_router_obj, post_map_func=post_map_person_func)

    person_runner_obj.run()

    # Generate look up for encounter_id to visit_occurrence_id
    person_json_file_name = create_json_map_from_csv_file(output_person_csv, "person_source_value", "person_id")
    empi_id_mapper = CoderMapperJSONClass(person_json_file_name)

    #### DEATH RECORD ####
    death_rules = create_death_patient_rules(json_map_directory, empi_id_mapper)

    output_death_csv = os.path.join(output_csv_directory, "death_cdm.csv")
    death_runner_obj = generate_mapper_obj(input_person_csv, PHDPersonObject(), output_death_csv, DeathObject(),
                                           death_rules, output_class_obj, in_out_map_obj, death_router_obj)
    death_runner_obj.run()

    #### Observation Period ####


    obs_per_rules = create_observation_period_rules(json_map_directory, empi_id_mapper)

    output_obs_per_csv = os.path.join(output_csv_directory, "observation_period_cdm.csv")

    input_obs_per_csv = os.path.join(input_csv_directory, "EMPI_ID_Oberservation_Period.csv")

    def obs_router_obj(input_dict):
        return ObservationPeriodObject()

    obs_per_runner_obj = generate_mapper_obj(input_obs_per_csv, EmpIdObservationPeriod(), output_obs_per_csv, ObservationPeriodObject(),
                                           obs_per_rules, output_class_obj, in_out_map_obj, obs_router_obj)
    obs_per_runner_obj.run()

    #### VISIT ####
    visit_rules = create_visit_rules(json_map_directory, empi_id_mapper)
    input_encounter_csv = os.path.join(input_csv_directory, "PH_F_Encounter.csv")
    output_visit_occurrence_csv = os.path.join(output_csv_directory, "visit_occurrence_cdm.csv")

    def visit_router_obj(input_dict):
        if len(empi_id_mapper.map({"empi_id": input_dict["empi_id"]})):
            if input_dict["classification_display"] not in ("Inbox Message"):
                return VisitOccurrenceObject()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    visit_runner_obj = generate_mapper_obj(input_encounter_csv, PHFEncounterObject(), output_visit_occurrence_csv,
                                           VisitOccurrenceObject(), visit_rules,
                                           output_class_obj, in_out_map_obj, visit_router_obj)

    visit_runner_obj.run()

    # Visit ID Map
    encounter_json_file_name = create_json_map_from_csv_file(output_visit_occurrence_csv, "visit_source_value", "visit_occurrence_id")
    encounter_id_mapper = CoderMapperJSONClass(encounter_json_file_name, "encounter_id")

    #### MEASUREMENT and OBSERVATIONS dervived from PH_F_Result ####
    snomed_code_json = os.path.join(json_map_directory, "CONCEPT_CODE_SNOMED.json")
    snomed_code_mapper = CoderMapperJSONClass(snomed_code_json)
    snomed_code_result_mapper = ChainMapper(FilterHasKeyValueMapper(["result_code"]), snomed_code_mapper)

    def measurement_router_obj(input_dict):
        """Determine if the result contains a LOINC code"""
        if len(empi_id_mapper.map({"empi_id": input_dict["empi_id"]})):
            if len(input_dict["result_code"]):
                mapped_result_code = snomed_code_result_mapper.map(input_dict)
                if "CONCEPT_CLASS_ID" in mapped_result_code:
                    if mapped_result_code["DOMAIN_ID"] == "Measurement":
                        return MeasurementObject()
                    elif mapped_result_code["DOMAIN_ID"] == "Observation":
                        return ObservationObject() #TODO: Support generation of observations e.g. Body weight, height from PH_F_Result
                    else:
                        return NoOutputClass()
                else:
                    return MeasurementObject()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    snomed_json = os.path.join(json_map_directory, "CONCEPT_NAME_SNOMED.json")  # We don't need the entire SNOMED
    snomed_mapper = CoderMapperJSONClass(snomed_json)

    measurement_rules, observation_measurement_rules = \
        create_measurement_and_observation_rules(json_map_directory, empi_id_mapper, encounter_id_mapper, snomed_mapper, snomed_code_mapper)

    input_result_csv = os.path.join(input_csv_directory, "PH_F_Result.csv")
    output_measurement_csv = os.path.join(output_csv_directory, "measurement_encounter_cdm.csv")

    measurement_runner_obj = generate_mapper_obj(input_result_csv, PHFResultObject(), output_measurement_csv, MeasurementObject(),
                                                 measurement_rules, output_class_obj, in_out_map_obj, measurement_router_obj)

    output_observation_csv = os.path.join(output_csv_directory, "observation_measurement_encounter_cdm.csv")
    register_to_mapper_obj(input_result_csv, PHFResultObject(), output_observation_csv,
                           ObservationObject(), observation_measurement_rules, output_class_obj, in_out_map_obj)

    measurement_runner_obj.run()

    #### CONDITION / DX ####

    condition_type_name_json = os.path.join(json_map_directory, "CONCEPT_NAME_Condition_Type.json")
    condition_type_name_map = CoderMapperJSONClass(condition_type_name_json)

    condition_claim_type_map = \
        ChainMapper(
            ReplacementMapper({"PRIMARY": "Primary Condition", "SECONDARY": "Secondary Condition"}),
            condition_type_name_map
        )

    #Claim IDs
    map_claim_id_encounter = os.path.join(input_csv_directory, "Map_Between_Claim_Id_Encounter_Id.csv")
    map_claim_id_encounter_json = create_json_map_from_csv_file(map_claim_id_encounter, "claim_uid", "encounter_id")
    claim_id_encounter_id_mapper = CoderMapperJSONClass(map_claim_id_encounter_json, "claim_id")

    claim_id_visit_occurrence_id_mapper = ChainMapper(claim_id_encounter_id_mapper, encounter_id_mapper)

    encounter_id_claim_id_mapper = CascadeKeyMapper("visit_occurrence_id", claim_id_visit_occurrence_id_mapper, encounter_id_mapper)

    input_condition_csv = os.path.join(input_csv_directory, "PH_F_Condition.csv")
    hi_condition_csv_obj = InputClassCSVRealization(input_condition_csv, PHFConditionObject())

    output_condition_csv = os.path.join(output_csv_directory, "condition_occurrence_dx_cdm.csv")
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
    condition_encounter_mapper = ChainMapper(ConstantMapper({"diagnosis_type_name": "Observation recorded from EHR"}), condition_type_name_map)

    condition_type_concept_mapper = CascadeMapper(condition_claim_type_map, condition_encounter_mapper)

    ICDMapper = CaseMapper(case_mapper_icd9_icd10, CoderMapperJSONClass(icd9cm_json, "condition_raw_code"), CoderMapperJSONClass(icd10cm_json, "condition_raw_code"))
    # Required: condition_occurrence_id, person_id, condition_concept_id, condition_start_date
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

    condition_rules_dx_class = build_input_output_mapper(condition_rules_dx)

    # ICD9 and ICD10 conditions which map to measurements according to the CDM Vocabulary
    in_out_map_obj.register(PHFConditionObject(), ConditionOccurrenceObject(), condition_rules_dx_class)
    output_directory_obj.register(ConditionOccurrenceObject(), cdm_condition_csv_obj)

    measurement_row_offset = measurement_runner_obj.rows_run
    measurement_rules_dx = [(":row_id", row_map_offset("measurement_id", measurement_row_offset),
                                      {"measurement_id": "measurement_id"}),
                                      (":row_id", ConstantMapper({"measurement_type_concept_id": 0}),
                                       {"measurement_type_concept_id": "measurement_type_concept_id"}),
                                      ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                      (("encounter_id", "claim_id"),
                                       encounter_id_claim_id_mapper,
                                       {"visit_occurrence_id": "visit_occurrence_id"}),
                                      ("effective_dt_tm", SplitDateTimeWithTZ(),
                                        {"date": "measurement_date", "time": "measurement_time"}),
                                      ("condition_code", "measurement_source_value"),
                                      (("condition_raw_code", "condition_coding_system_id"), ICDMapper,
                                       {"CONCEPT_ID": "measurement_source_concept_id",
                                        "MAPPED_CONCEPT_ID": "measurement_concept_id"})
                                      ]

    measurement_rules_dx_class = build_input_output_mapper(measurement_rules_dx)
    in_out_map_obj.register(PHFConditionObject(), MeasurementObject(), measurement_rules_dx_class)

    # The mapped ICD9 to measurements get mapped to a separate code
    output_measurement_dx_encounter_csv = os.path.join(output_csv_directory, "measurement_dx_cdm.csv")
    output_measurement_dx_encounter_csv_obj = OutputClassCSVRealization(output_measurement_dx_encounter_csv,
                                                                        MeasurementObject())

    output_directory_obj.register(MeasurementObject(), output_measurement_dx_encounter_csv_obj)

    observation_row_offset = measurement_runner_obj.rows_run

    # ICD9 and ICD10 codes which map to observations according to the CDM Vocabulary
    observation_rules_dx = [(":row_id", row_map_offset("observation_id", observation_row_offset),
                                       {"observation_id": "observation_id"}),
                                      (":row_id", ConstantMapper({"observation_type_concept_id": 0}),
                                       {"observation_type_concept_id": "observation_type_concept_id"}),
                                      ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                      (("encounter_id", "claim_id"),
                                       encounter_id_claim_id_mapper,
                                       {"visit_occurrence_id": "visit_occurrence_id"}),
                                      ("effective_dt_tm", SplitDateTimeWithTZ(),
                                       {"date": "observation_date", "time": "observation_time"}),
                                      ("condition_code", "observation_source_value"),
                                      (("condition_raw_code", "condition_coding_system_id"), ICDMapper,
                                      {"CONCEPT_ID": "observation_source_concept_id",
                                       "MAPPED_CONCEPT_ID": "observation_concept_id"}),
                                      (("rank_type",), condition_claim_type_map,
                                       {"CONCEPT_ID": "condition_type_concept_id"})
                                      ]

    observation_rules_dx_class = build_input_output_mapper(observation_rules_dx)

    output_observation_dx_encounter_csv = os.path.join(output_csv_directory, "observation_dx_cdm.csv")
    output_observation_dx_encounter_csv_obj = OutputClassCSVRealization(output_observation_dx_encounter_csv,
                                                                        ObservationObject())

    output_directory_obj.register(ObservationObject(), output_observation_dx_encounter_csv_obj)
    in_out_map_obj.register(PHFConditionObject(), ObservationObject(), observation_rules_dx_class)

    procedure_type_json = os.path.join(json_map_directory, "CONCEPT_NAME_Procedure_Type.json")
    procedure_type_mapper = CoderMapperJSONClass(procedure_type_json)


    # ICD9 and ICD10 codes which map to procedures according to the CDM Vocabulary
    #"Procedure recorded as diagnostic code"
    # TODO: Map procedure_type_concept_id
    procedure_rules_dx_encounter = [(":row_id", "procedure_occurrence_id"),
                                      (":row_id", ChainMapper(ConstantMapper({"name": "Procedure recorded as diagnostic code"}),
                                                              procedure_type_mapper),
                                       {"CONCEPT_ID": "procedure_type_concept_id"}),
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

    output_procedure_dx_encounter_csv = os.path.join(output_csv_directory, "procedure_dx_cdm.csv")
    output_procedure_dx_encounter_csv_obj = OutputClassCSVRealization(output_procedure_dx_encounter_csv,
                                                                      ProcedureOccurrenceObject())

    output_directory_obj.register(ProcedureOccurrenceObject(), output_procedure_dx_encounter_csv_obj)
    in_out_map_obj.register(PHFConditionObject(), ProcedureOccurrenceObject(), procedure_rules_dx_encounter_class)

    def condition_router_obj(input_dict):
        """ICD9 / ICD10 CM contain codes which could either be a procedure, observation, or measurement"""
        coding_system_oid = input_dict["condition_coding_system_id"]
        if len(empi_id_mapper.map({"empi_id": input_dict["empi_id"]})):
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
        else:
            return NoOutputClass()

    condition_runner_obj = RunMapperAgainstSingleInputRealization(hi_condition_csv_obj, in_out_map_obj,
                                                                    output_directory_obj,
                                                                    condition_router_obj)

    condition_runner_obj.run()

    # Update needed offsets
    condition_row_offset = condition_runner_obj.rows_run
    procedure_row_offset = condition_runner_obj.rows_run
    measurement_row_offset += condition_row_offset


    #### PROCEDURE ENCOUNTER ####

    procedure_rules_encounter = create_procedure_rules(json_map_directory, empi_id_mapper, encounter_id_claim_id_mapper,
                                                       procedure_row_offset)
    procedure_rule = procedure_rules_encounter[0]
    procedure_code_map = procedure_rule[1]

    procedure_rules_encounter_class = build_input_output_mapper(procedure_rules_encounter)

    # Procedure codes which map to measurements according to the CDM Vocabulary

    measurement_rules_proc_encounter = [(":row_id", row_map_offset("measurement_id", measurement_row_offset),
                                      {"measurement_id": "measurement_id"}),
                                        ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                        (":row_id", ConstantMapper({"measurement_type_concept_id": 0}), #TODO: Add measurement_type_concept_id
                                         {"measurement_type_concept_id": "measurement_type_concept_id"}),
                                        (("encounter_id", "claim_id"),
                                            encounter_id_claim_id_mapper,
                                            {"visit_occurrence_id": "visit_occurrence_id"}),
                                        ("service_start_dt_tm", SplitDateTimeWithTZ(),
                                            {"date": "measurement_date", "time": "measurement_time"}),
                                        ("procedure_code", "measurement_source_value"),
                                        (("procedure_code", "procedure_coding_system_id"), procedure_code_map,
                                            {"CONCEPT_ID": "measurement_source_concept_id",
                                             "MAPPED_CONCEPT_ID": "measurement_concept_id"})
                                      ]

    measurement_rules_proc_encounter_class = build_input_output_mapper(measurement_rules_proc_encounter)

    output_measurement_proc_encounter_csv = os.path.join(output_csv_directory, "measurement_proc_cdm.csv")
    output_measurement_proc_encounter_csv_obj = OutputClassCSVRealization(output_measurement_proc_encounter_csv,
                                                                      MeasurementObject())
    output_directory_obj.register(MeasurementObject(), output_measurement_proc_encounter_csv_obj)

    in_out_map_obj.register(PHFProcedureObject(), MeasurementObject(), measurement_rules_proc_encounter_class)

    #TODO: Add: Device, Measurement, Observation, Procedure, DrugExposure

    input_proc_csv = os.path.join(input_csv_directory, "PH_F_Procedure.csv")
    hi_proc_csv_obj = InputClassCSVRealization(input_proc_csv, PHFProcedureObject())

    in_out_map_obj.register(PHFProcedureObject(), ProcedureOccurrenceObject(), procedure_rules_encounter_class)

    output_proc_encounter_csv = os.path.join(output_csv_directory, "procedure_cdm.csv")
    output_proc_encounter_csv_obj = OutputClassCSVRealization(output_proc_encounter_csv,
                                                                          ProcedureOccurrenceObject())

    output_directory_obj.register(ProcedureOccurrenceObject(), output_proc_encounter_csv_obj)

    def procedure_router_obj(input_dict):
        if len(empi_id_mapper.map({"empi_id": input_dict["empi_id"]})):
            #print(procedure_coding_system(input_dict))
            if "procedure_coding_system_id" in input_dict:
                if procedure_coding_system(input_dict) in ("ICD9 Procedure Codes", "ICD10 Procedure Codes", "CPT Codes", "HCPCS"):
                    result_dict = procedure_code_map.map(input_dict)
                    #print(procedure_coding_system(input_dict),input_dict, result_dict)
                    if "DOMAIN_ID" in result_dict:
                        domain = result_dict["DOMAIN_ID"]
                        if domain == "Procedure":
                            return ProcedureOccurrenceObject()
                        elif domain == "Measurement":
                            return MeasurementObject()
                        elif domain == "Observation":
                            return NoOutputClass() # ObservationObject()
                        elif domain == "Drug":
                            return NoOutputClass() # DrugExposure()
                        elif domain == "Device":
                            return NoOutputClass()  # DeviceExposure()
                        else:
                            return NoOutputClass()
                    else:
                        return NoOutputClass()
                else:
                    return NoOutputClass()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    procedure_runner_obj = RunMapperAgainstSingleInputRealization(hi_proc_csv_obj, in_out_map_obj,
                                                                    output_directory_obj,
                                                                    procedure_router_obj)

    procedure_runner_obj.run()

    drug_row_offset = procedure_runner_obj.rows_run

    #### DRUG EXPOSURE ####
    def drug_exposure_router_obj(input_dict):
        """Route mapping of drug_exposure"""
        if len(empi_id_mapper.map({"empi_id": input_dict["empi_id"]})):
            if input_dict["status_primary_display"] not in ("Deleted", "Cancelled"):
                if len(input_dict["drug_raw_code"]) > 0 and len(input_dict["drug_primary_display"]) > 0:
                    return DrugExposureObject()
                else:
                    return NoOutputClass()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    def drug_post_processing(output_dict):
        """For concept_id"""
        fields = ["drug_concept_id", "drug_source_concept_id"]
        for field in fields:
            if field not in output_dict:
                output_dict[field] = 0
            else:
                if output_dict[field] is not None:
                    if not len(output_dict[field]):
                        output_dict[field] = 0
        return output_dict

    input_med_csv = os.path.join(input_csv_directory, "PH_F_Medication.csv")
    output_drug_exposure_csv = os.path.join(output_csv_directory, "drug_exposure_cdm.csv")

    medication_rules = create_medication_rules(json_map_directory, empi_id_mapper, encounter_id_mapper, snomed_mapper) #TODO: add drug_row_offset

    drug_exposure_runner_obj = generate_mapper_obj(input_med_csv, PHFMedicationObject(), output_drug_exposure_csv, DrugExposureObject(),
                                                   medication_rules, output_class_obj, in_out_map_obj, drug_exposure_router_obj,
                                                   post_map_func=drug_post_processing)

    drug_exposure_runner_obj.run()

    #  TODO: Add MS-DRGS

    # ["observation_id", "person_id", "observation_concept_id", "observation_date", "observation_time",
    #  "observation_type_concept_id", "value_as_number", "value_as_string", "value_as_concept_id", "qualifier_concept_id",
    #  "unit_concept_id", "provider_id", "visit_occurrence_id", "observation_source_value",
    #  "observation_source_concept_id", "unit_source_value", "qualifier_source_value"]


if __name__ == "__main__":
    with open("hi_config.json", "r") as f:
        config_dict = json.load(f)

    main(config_dict["csv_input_directory"], config_dict["csv_output_directory"], config_dict["json_map_directory"])