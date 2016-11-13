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


def create_visit_rules(json_map_directory, empi_id_mapper):
    """Generate rules for mapping PH_F_Encounter to VisitOccurrence"""

    visit_concept_json = os.path.join(json_map_directory, "CONCEPT_NAME_Visit.json")
    visit_concept_mapper = ChainMapper(
        ReplacementMapper({"Inpatient": "Inpatient Visit", "Emergency": "Emergency Room Visit",
                           "Outpatient": "Outpatient Visit", "Observation": "Emergency Room Visit"}), # Note: there are no observation type
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


def create_measurement_rules(json_map_directory, empi_id_mapper, encounter_id_mapper, snomed_mapper, snomed_code_mapper):
    """Generate rules for mapping PH_F_Result to Measurement"""

    loinc_json = os.path.join(json_map_directory, "LOINC_with_parent.json")
    loinc_mapper = CoderMapperJSONClass(loinc_json)

    measurement_type_json = os.path.join(json_map_directory, "CONCEPT_NAME_Meas_Type.json")
    measurement_type_mapper = CoderMapperJSONClass(measurement_type_json)

    unit_measurement_mapper = ChainMapper(ReplacementMapper({"s": "__s__"}), snomed_mapper)

    # TODO Add SNOMED Mapper
    # TODO: mapping for "measurement_type_concept_id"
    # "Derived value" "From physical examination"  "Lab result"  "Pathology finding"   "Patient reported value"   "Test ordered through EHR"
    # "CONCEPT_CLASS_ID": "Lab Test"

    # TODO: Add value_as_concept_id
    # TODO: Will need to map standard concepts to values

    measurement_code_mapper = CascadeMapper(loinc_mapper, snomed_code_mapper, ConstantMapper({"CONCEPT_ID": 0}))

    measurement_type_chained_mapper = CascadeMapper(ChainMapper(loinc_mapper, FilterHasKeyValueMapper(["CONCEPT_CLASS_ID"]),
                                                                 ReplacementMapper({"Lab Test": "Lab result"}),
                                                                 measurement_type_mapper), ConstantMapper({"CONCEPT_ID": 0}))
    value_as_concept_mapper = ChainMapper(FilterHasKeyValueMapper(["norm_codified_value_primary_display", "norm_text_value"]),
                                          TransformMapper(capitalize_words_and_normalize_spacing),
                                          ReplacementMapper({"Implant": "implant", "A": "Blood group A", "Ab": "Blood group AB", "O": "Blood group O"}),
                                          snomed_mapper)
    numeric_coded_mapper = FilterHasKeyValueMapper(["numeric_value", "norm_codified_value_primary_display", "norm_text_value", "result_primary_display"])

    measurement_rules = [(":row_id", "measurement_id"),
                         ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                         ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                         ("service_date", SplitDateTimeWithTZ(), {"date": "measurement_date", "time": "measurement_time"}),
                         ("result_code", "measurement_source_value"),  # TODO: Add logic norm_codified_value_display
                         ("result_code", measurement_code_mapper,  {"CONCEPT_ID": "measurement_source_concept_id"}),
                         ("result_code", measurement_code_mapper,  {"CONCEPT_ID": "measurement_concept_id"}),
                         ("result_code", measurement_type_chained_mapper, {"CONCEPT_ID": "measurement_type_concept_id"}),
                         ("numeric_value", FloatMapper(), "value_as_number"),
                         (("norm_text_value", "norm_codified_value_primary_display"), value_as_concept_mapper, {"CONCEPT_ID": "value_as_concept_id"}), #norm_codified_value_primary_display",
                         ("norm_unit_of_measure_primary_display", "unit_source_value"),
                         ("norm_unit_of_measure_primary_display", unit_measurement_mapper, {"CONCEPT_ID": "unit_concept_id"}),
                         (("numeric_value", "norm_codified_value_primary_display", "result_primary_display", "norm_text_value"), numeric_coded_mapper,
                          {"numeric_value": "value_source_value",
                           "norm_codified_value_primary_display": "value_source_value",
                           "result_primary_display": "value_source_value",
                           "norm_text_value": "value_source_value"}),
                         ("norm_ref_range_low", FloatMapper(), "range_low"),  # TODO: Some values contain non-numeric elements
                         ("norm_ref_range_high", FloatMapper(), "range_high")]

    return measurement_rules


def create_procedure_rules(json_map_directory, empi_id_mapper, encounter_id_mapper):
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
    procedure_type_name_json = os.path.join(json_map_directory, "CONCEPT_NAME_Procedure_Type.json")

    procedure_type_map = \
        ChainMapper(
            ReplacementMapper({"PRIMARY": "Primary Procedure", "SECONDARY": "Secondary Procedure"}),
            CoderMapperJSONClass(procedure_type_name_json)
        )


    # TODO: Add SNOMED and HCPCS Codes to the Mapping
    ProcedureCodeMapper = CaseMapper(case_mapper_procedures,
                                     CoderMapperJSONClass(icd9proc_json, "procedure_raw_code"),
                                     CoderMapperJSONClass(icd10proc_json, "procedure_raw_code"),
                                     CoderMapperJSONClass(cpt_json, "procedure_raw_code")
                                     )

    # Required: procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id
    procedure_rules_encounter = [(":row_id", "procedure_occurrence_id"),
                                 ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                                 ("encounter_id", encounter_id_mapper,
                                  {"visit_occurrence_id": "visit_occurrence_id"}),
                                 ("service_start_dt_tm", SplitDateTimeWithTZ(),
                                  {"date": "procedure_date"}),
                                 ("procedure_raw_code", "procedure_source_value"),
                                 (("procedure_raw_code", "procedure_coding_system_id"), ProcedureCodeMapper,
                                  {"CONCEPT_ID": "procedure_source_concept_id",
                                   "MAPPED_CONCEPT_ID": "procedure_concept_id"},
                                  ),
                                 ("rank_type", procedure_type_map, {"CONCEPT_ID": "procedure_type_concept_id"})
                                 ]

    return procedure_rules_encounter


def create_medication_rules(json_map_directory, empi_id_mapper, encounter_id_mapper, snomed_mapper):
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
                                                      "UNKNOWN": "Prescription dispensed in pharmacy"}),
                                   drug_type_code_mapper)

    # TODO: Rework this mapper
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
                        (("drug_raw_coding_system_id", "drug_raw_code", "drug_primary_display"), drug_mapper,
                         {"CONCEPT_ID": "drug_source_concept_id"}),
                        (("drug_raw_coding_system_id", "drug_raw_code", "drug_primary_display"), drug_mapper,
                         {"CONCEPT_ID": "drug_concept_id"}),
                        ("intended_dispenser", drug_type_mapper, {"CONCEPT_ID": "drug_type_concept_id"})
                        ]

    return medication_rules


def main(input_csv_directory, output_csv_directory, json_map_directory):
    # TODO: Add Provider
    # TODO: Add Patient Location

    output_class_obj = OutputClassDirectory()

    in_out_map_obj = InputOutputMapperDirectory()
    output_directory_obj = OutputClassDirectory()

    #### Person ####
    patient_rules = create_patient_rules(json_map_directory)

    input_person_csv = os.path.join(input_csv_directory, "PH_D_Person.csv")
    output_person_csv = os.path.join(output_csv_directory, "person_cdm.csv")

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

    #### DEATH ####
    death_rules = create_death_patient_rules(json_map_directory, empi_id_mapper)

    output_death_csv = os.path.join(output_csv_directory, "death_cdm.csv")
    death_runner_obj = generate_mapper_obj(input_person_csv, PHDPersonObject(), output_death_csv, DeathObject(),
                                           death_rules, output_class_obj, in_out_map_obj, death_router_obj)
    death_runner_obj.run()

    #### VISIT ####

    visit_rules = create_visit_rules(json_map_directory, empi_id_mapper)
    input_encounter_csv = os.path.join(input_csv_directory, "PH_F_Encounter.csv")
    output_visit_occurrence_csv = os.path.join(output_csv_directory, "visit_occurrence_cdm.csv")

    def visit_router_obj(input_dict):
        if len(empi_id_mapper.map({"empi_id": input_dict["empi_id"]})):
            return VisitOccurrenceObject()
        else:
            return NoOutputClass()

    visit_runner_obj = generate_mapper_obj(input_encounter_csv, PHFEncounterObject(), output_visit_occurrence_csv,
                                           VisitOccurrenceObject(), visit_rules,
                                           output_class_obj, in_out_map_obj, visit_router_obj)

    visit_runner_obj.run()

    # Visit ID Map
    encounter_json_file_name = create_json_map_from_csv_file(output_visit_occurrence_csv, "visit_source_value", "visit_occurrence_id")
    encounter_id_mapper = CoderMapperJSONClass(encounter_json_file_name)

    #### MEASUREMENT ####


    snomed_code_json = os.path.join(json_map_directory, "CONCEPT_CODE_SNOMED.json")
    snomed_code_mapper = CoderMapperJSONClass(snomed_code_json)
    snomed_code_result_mapper = ChainMapper(FilterHasKeyValueMapper(["result_code"]), snomed_code_mapper)

    def measurement_router_obj(input_dict):
        """Determine if the result contains a LOINC code"""
        if len(empi_id_mapper.map({"empi_id": input_dict["empi_id"]})):

            if len(input_dict["result_code"]):
                mapped_result_code = snomed_code_result_mapper.map(input_dict)
                if "CONCEPT_CLASS_ID" in mapped_result_code:
                    if mapped_result_code["CONCEPT_CLASS_ID"] in ("Procedure", "Clinical Finding", "Staging / Scales"):
                        return NoOutputClass()
                    else:
                        return MeasurementObject()
                else:
                    return MeasurementObject()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    snomed_json = os.path.join(json_map_directory, "CONCEPT_NAME_SNOMED.json")  # We don't need the entire SNOMED
    snomed_mapper = CoderMapperJSONClass(snomed_json)

    measurement_rules = create_measurement_rules(json_map_directory, empi_id_mapper, encounter_id_mapper, snomed_mapper, snomed_code_mapper)

    input_result_csv = os.path.join(input_csv_directory, "PH_F_Result.csv")
    output_measurement_csv = os.path.join(output_csv_directory, "measurement_cdm_encounter.csv")

    measurement_runner_obj = generate_mapper_obj(input_result_csv, PHFResultObject(), output_measurement_csv, MeasurementObject(),
                                                 measurement_rules, output_class_obj, in_out_map_obj, measurement_router_obj)
    measurement_runner_obj.run()

    #### CONDITION / DX ####

    condition_type_name_json = os.path.join(json_map_directory, "CONCEPT_NAME_Condition_Type.json")
    condition_type_name_map = CoderMapperJSONClass(condition_type_name_json)

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
    condition_encounter_mapper = ChainMapper(ConstantMapper({"diagnosis_type_name": "Observation recorded from EHR"}), condition_type_name_map)

    ICDMapper = CaseMapper(case_mapper_icd9_icd10, CoderMapperJSONClass(icd9cm_json, "condition_raw_code"), CoderMapperJSONClass(icd10cm_json, "condition_raw_code"))
    # Required: condition_occurrence_id, person_id, condition_concept_id, condition_start_date
    condition_rules_dx_encounter = [(":row_id", "condition_occurrence_id"),
                       ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                       ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                       (("condition_raw_code", "condition_coding_system_id"), ICDMapper, {"CONCEPT_ID": "condition_source_concept_id", "MAPPED_CONCEPT_ID": "condition_concept_id"}),
                       ("condition_raw_code", "condition_source_value"),
                       ("classification_primary_display", condition_encounter_mapper, {"CONCEPT_ID": "condition_type_concept_id"}),
                       ("effective_dt_tm", SplitDateTimeWithTZ(), {"date": "condition_start_date"})]

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
                                      (("condition_raw_code", "condition_coding_system_id"), ICDMapper,
                                       {"CONCEPT_ID": "measurement_source_concept_id",
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

    condition_row_offset = condition_runner_obj.rows_run

    #### CONDITION FROM CLAIM ####

    map_claim_id_encounter = os.path.join(input_csv_directory, "Map_Between_Claim_Id_Encounter_Id.csv")

    map_claim_id_encounter_json = create_json_map_from_csv_file(map_claim_id_encounter, "claim_id", "encounter_id")

    claim_id_encounter_id_mapper = CoderMapperJSONClass(map_claim_id_encounter_json)
    claim_id_visit_occurrence_id_mapper = ChainMapper(claim_id_encounter_id_mapper, encounter_id_mapper)

    condition_claim_type_map = \
        ChainMapper(
            ReplacementMapper({"PRIMARY": "Primary Condition", "SECONDARY": "Secondary Condition"}),
            condition_type_name_map
        )

    condition_claim_rules = [(":row_id", row_map_offset("condition_occurrence_id", condition_row_offset),
                              {"condition_occurrence_id": "condition_occurrence_id"}),
                             ("corrected_claim_id", claim_id_visit_occurrence_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                             ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                             (("condition_raw_code", "condition_coding_system_id"), ICDMapper,
                              {"CONCEPT_ID": "condition_source_concept_id",
                               "MAPPED_CONCEPT_ID": "condition_concept_id"}),
                             ("condition_raw_code", "condition_source_value"),
                             (("rank_type",), condition_claim_type_map, {"CONCEPT_ID": "condition_type_concept_id"}),
                             ("effective_dt_tm", SplitDateTimeWithTZ(), {"date": "condition_start_date"})
                             ]

    def condition_claim_router(input_dict):
        # we need to check
        claim_id_mapped_dict = claim_id_encounter_id_mapper.map({"corrected_claim_id": input_dict["corrected_claim_id"]})
        if len(empi_id_mapper.map({"empi_id": input_dict["empi_id"]})):
            if len(input_dict["present_on_admission_code"]): # Remove these blanks are admit codes
                if len(claim_id_mapped_dict):
                    claim_encounter_id_mapped_dict = encounter_id_mapper.map(claim_id_mapped_dict)
                    if len(claim_encounter_id_mapped_dict):
                        return ConditionOccurrenceObject()
                    else:
                        return NoOutputClass()
                else:
                    return NoOutputClass()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()

    input_condition_claim_csv = os.path.join(input_csv_directory, "PH_F_Condition_Claim.csv")
    output_condition_occurrence_claim_csv = os.path.join(output_csv_directory, "condition_occurrence_cdm_claim.csv")

    condition_claim_runner_obj = generate_mapper_obj(input_condition_claim_csv, PHFConditionClaimObject(),
                                                     output_condition_occurrence_claim_csv, ConditionOccurrenceObject(),
                                                     condition_claim_rules, output_class_obj, in_out_map_obj, condition_claim_router
                                                     )
    condition_claim_runner_obj.run()

    #### PROCEDURE ENCOUNTER ####
    def procedure_router_obj(input_dict):
        if len(empi_id_mapper.map({"empi_id": input_dict["empi_id"]})):
            if procedure_coding_system(input_dict) in ("ICD9 Procedure Codes", "ICD10 Procedure Codes", "CPT Codes"):
                return ProcedureOccurrenceObject()
            else:
                return NoOutputClass()
        else:
            return NoOutputClass()



    procedure_rules_encounter = create_procedure_rules(json_map_directory, empi_id_mapper, encounter_id_mapper)

    input_proc_csv = os.path.join(input_csv_directory, "PH_F_Procedure.csv")
    output_procedure_encounter_csv = os.path.join(output_csv_directory, "procedure_encounter_cdm.csv")

    procedure_runner_obj = generate_mapper_obj(input_proc_csv, PHFProcedureObject(), output_procedure_encounter_csv,
                                               ProcedureOccurrenceObject(), procedure_rules_encounter,
                                               output_class_obj, in_out_map_obj, procedure_router_obj
                                               )

    procedure_runner_obj.run()

    #### DRUG EXPOSURE ####
    def drug_exposure_router_obj(input_dict):
        """Route mapping of drug_exposure"""
        if len(empi_id_mapper.map({"empi_id": input_dict["empi_id"]})):
            if input_dict["status_primary_display"] not in ("Deleted", "Cancelled"):
                return DrugExposureObject()
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
                if not len(output_dict[field]):
                    output_dict[field] = 0
        return output_dict

    input_med_csv = os.path.join(input_csv_directory, "PH_F_Medication.csv")
    output_drug_exposure_csv = os.path.join(output_csv_directory, "drug_exposure_cdm.csv")

    medication_rules = create_medication_rules(json_map_directory, empi_id_mapper, encounter_id_mapper, snomed_mapper)

    drug_exposure_runner_obj = generate_mapper_obj(input_med_csv, PHFMedicationObject(), output_drug_exposure_csv, DrugExposureObject(),
                                                   medication_rules, output_class_obj, in_out_map_obj, drug_exposure_router_obj,
                                                   post_map_func=drug_post_processing)

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