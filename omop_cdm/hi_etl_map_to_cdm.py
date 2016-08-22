
from omop_cdm_functions import *
from omop_cdm_classes import *
from hi_classes import *
import os
from mapping_classes import *


def person_router_obj(input_dict):
    return PersonObject()


def death_router_obj(input_dict):
    "Determines if a row_dict codes a death"
    if input_dict["deceased"] == "true":
        return DeathObject()
    else:
        return NoOutputClass()

def visit_router_obj(input_dict):
    return VisitOccurrenceObject()

def measurement_router_obj(input_dict):
    "Determine if the result is a LOINC code"
    if "-" in input_dict["result_code"]:
        return MeasurementObject()
    else:
        return NoOutputClass()


def icd9_versus_icd10_coding(coding_system_oid):
    if coding_system_oid == "2.16.840.1.113883.6.90":
        return "ICD10CM"
    elif coding_system_oid == "2.16.840.1.113883.6.103":
        return "ICD9CM"
    else:
        return False


def case_mapper_icd9_icd10(input_dict, field="condition_coding_system_id"):

    coding_system_oid = input_dict[field]
    coding_version = icd9_versus_icd10_coding(coding_system_oid)

    if coding_version == "ICD9CM":
        return 0
    else:
        return 1


def main(input_csv_directory, output_csv_directory, json_map_directory):

    # Provider

    # Location

    gender_json = os.path.join(json_map_directory, "CONCEPT_NAME_Gender.json")
    gender_json_mapper = CoderMapperJSONClass(gender_json)
    upper_case_mapper = TransformMapper(lambda x: x.upper())
    gender_mapper = ChainMapper(upper_case_mapper, gender_json_mapper)

    # Person input / output

    input_person_csv = os.path.join(input_csv_directory, "PH_D_Person.csv")
    hi_person_csv_obj = InputClassCSVRealization(input_person_csv, PHDPersonObject())

    output_person_csv = os.path.join(output_csv_directory, "person_cdm.csv")
    cdm_person_csv_obj = OutputClassCSVRealization(output_person_csv, PersonObject())

    # time_of_birth
    # race_concept_id
    # ethnicity_concept_id
    # location_id
    # provider_id
    # care_site_id

    # Person input mapper
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

    # visit_occurrence

    # ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date", "visit_start_time", "visit_end_date",
    # "visit_end_time", "visit_type_concept_id", "provider_id", "care_site_id", "visit_source_value", "visit_source_concept_id"]

    visit_concept_json = os.path.join(json_map_directory, "CONCEPT_NAME_Visit.json")
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

    output_measurement_csv = os.path.join(output_csv_directory, "measurement_cdm.csv")
    cdm_measurement_csv_obj = OutputClassCSVRealization(output_measurement_csv, MeasurementObject())

    loinc_json = os.path.join(json_map_directory, "LOINC_with_parent.json")
    loinc_mapper = CoderMapperJSONClass(loinc_json)

    # TODO: mapping for "measurement_type_concept_id"
    # "Derived value" "From physical examination"  "Lab result"  "Pathology finding"   "Patient reported value"   "Test ordered through EHR"
    # "CONCEPT_CLASS_ID": "Lab Test"

    measurement_rules = [(":row_id", "measurement_id"),
                         ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                         ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                         ("service_date", SplitDateTimeWithTZ(), {"date": "measurement_date", "time": "measurement_time"}),
                         ("result_code", "measurement_source_value"), # TODO Add logic norm_codified_value_display
                         ("result_code", loinc_mapper, {"CONCEPT_ID": "measurement_source_concept_id", "MAPPED_CONCEPT_ID": "measurement_concept_id"}),
                         ("numeric_value", "value_as_number"),
                         ("norm_unit_of_measure_primary_display", "unit_source_value"),
                         ("result_primary_display", "value_source_value"),
                         ("norm_ref_range_low", "range_low"), #TODO Some values contain non-numeric elements
                         ("norm_ref_range_high", "range_high")]

    measurement_rules_class = build_input_output_mapper(measurement_rules)

    in_out_map_obj.register(PHFResultObject(), MeasurementObject(), measurement_rules_class)
    output_directory_obj.register(MeasurementObject(), cdm_measurement_csv_obj)

    measurement_runner_obj = RunMapperAgainstSingleInputRealization(hi_result_csv_obj, in_out_map_obj,
                                                              output_directory_obj,
                                                              measurement_router_obj)
    measurement_runner_obj.run()

    # PH_F_Condition

    #ConditionOccurrenceObject()

    # Need to map ICD9CM versus ICD10CM DX codes
    # Need to take into account domain issues

    # condition_id
    # Admitting
    # Billing
    # Diagnosis
    # Discharge
    # Final
    # Other
    # Reason
    # For
    # Visit
    # Working

    # "CONCEPT_NAME_Condition_Type.json

    #condition_start_date
    #effective_dt_tm

    #condition_source_value
    #condition_raw_code


    input_condition_csv = os.path.join(input_csv_directory, "PH_F_Condition.csv")
    hi_condition_csv_obj = InputClassCSVRealization(input_condition_csv, PHFConditionObject())

    output_condition_csv = os.path.join(output_csv_directory, "condition_occurrence_cdm.csv")
    cdm_condition_csv_obj = OutputClassCSVRealization(output_condition_csv, ConditionOccurrenceObject())

    icd9cm_json = os.path.join(json_map_directory, "ICD9CM_with_parent.json")
    icd10cm_json = os.path.join(json_map_directory, "ICD10CM_with_parent.json")

    ICDMapper = CaseMapper(case_mapper_icd9_icd10, CoderMapperJSONClass(icd9cm_json, "condition_raw_code"), CoderMapperJSONClass(icd10cm_json, "condition_raw_code"))

    condition_rules = [(":row_id", "condition_occurrence_id"),
                       ("empi_id", empi_id_mapper, {"person_id": "person_id"}),
                       ("encounter_id", encounter_id_mapper, {"visit_occurrence_id": "visit_occurrence_id"}),
                       (("condition_raw_code","condition_coding_system_id"), ICDMapper, {"CONCEPT_ID": "condition_source_concept_id", "MAPPED_CONCEPT_ID": "condition_concept_id"}),
                       ("condition_raw_code", "condition_source_value"),
                       ("effective_dt_tm", SplitDateTimeWithTZ(), {"date": "condition_start_date"})
                      ]

    condition_rules_class = build_input_output_mapper(condition_rules)

    in_out_map_obj.register(PHFConditionObject(), ConditionOccurrenceObject(), condition_rules_class)
    output_directory_obj.register(ConditionOccurrenceObject(), cdm_condition_csv_obj)

    def condition_router_obj(input_dict):
        coding_system_oid = input_dict["condition_coding_system_id"]

        if coding_system_oid:
            result_dict = ICDMapper.map(input_dict)
            if result_dict != {}:
                if result_dict["DOMAIN_ID"] == "Condition":
                    return ConditionOccurrenceObject()
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

    [u'Carrier claim detail - 10th position',
     u'Carrier claim detail - 11th position',
     u'Carrier claim detail - 12th position',
     u'Carrier claim detail - 13th position',
     u'Carrier claim detail - 1st position',
     u'Carrier claim detail - 2nd position',
     u'Carrier claim detail - 3rd position',
     u'Carrier claim detail - 4th position',
     u'Carrier claim detail - 5th position',
     u'Carrier claim detail - 6th position',
     u'Carrier claim detail - 7th position',
     u'Carrier claim detail - 8th position',
     u'Carrier claim detail - 9th position',
     u'Carrier claim header - 1st position',
     u'Carrier claim header - 2nd position',
     u'Carrier claim header - 3rd position',
     u'Carrier claim header - 4th position',
     u'Carrier claim header - 5th position',
     u'Carrier claim header - 6th position',
     u'Carrier claim header - 7th position',
     u'Carrier claim header - 8th position',
     u'Condition era - 0 days persistence window',
     u'Condition era - 30 days persistence window',
     u'EHR Chief Complaint',
     u'EHR Episode Entry',
     u'EHR problem list entry',
     u'First Position Condition',
     u'Inpatient detail - 10th position',
     u'Inpatient detail - 11th position',
     u'Inpatient detail - 12th position',
     u'Inpatient detail - 13th position',
     u'Inpatient detail - 14th position',
     u'Inpatient detail - 15th position',
     u'Inpatient detail - 16th position',
     u'Inpatient detail - 17th position',
     u'Inpatient detail - 18th position',
     u'Inpatient detail - 19th position',
     u'Inpatient detail - 1st position',
     u'Inpatient detail - 20th position',
     u'Inpatient detail - 2nd position',
     u'Inpatient detail - 3rd position',
     u'Inpatient detail - 4th position',
     u'Inpatient detail - 5th position',
     u'Inpatient detail - 6th position',
     u'Inpatient detail - 7th position',
     u'Inpatient detail - 8th position',
     u'Inpatient detail - 9th position',
     u'Inpatient detail - primary',
     u'Inpatient header - 10th position',
     u'Inpatient header - 11th position',
     u'Inpatient header - 12th position',
     u'Inpatient header - 13th position',
     u'Inpatient header - 14th position',
     u'Inpatient header - 15th position',
     u'Inpatient header - 1st position',
     u'Inpatient header - 2nd position',
     u'Inpatient header - 3rd position',
     u'Inpatient header - 4th position',
     u'Inpatient header - 5th position',
     u'Inpatient header - 6th position',
     u'Inpatient header - 7th position',
     u'Inpatient header - 8th position',
     u'Inpatient header - 9th position',
     u'Inpatient header - primary',
     u'Observation recorded from EHR',
     u'Outpatient detail - 10th position',
     u'Outpatient detail - 11th position',
     u'Outpatient detail - 12th position',
     u'Outpatient detail - 13th position',
     u'Outpatient detail - 14th position',
     u'Outpatient detail - 15th position',
     u'Outpatient detail - 1st position',
     u'Outpatient detail - 2nd position',
     u'Outpatient detail - 3rd position',
     u'Outpatient detail - 4th position',
     u'Outpatient detail - 5th position',
     u'Outpatient detail - 6th position',
     u'Outpatient detail - 7th position',
     u'Outpatient detail - 8th position',
     u'Outpatient detail - 9th position',
     u'Outpatient header - 10th position',
     u'Outpatient header - 11th position',
     u'Outpatient header - 12th position',
     u'Outpatient header - 13th position',
     u'Outpatient header - 14th position',
     u'Outpatient header - 15th position',
     u'Outpatient header - 1st position',
     u'Outpatient header - 2nd position',
     u'Outpatient header - 3rd position',
     u'Outpatient header - 4th position',
     u'Outpatient header - 5th position',
     u'Outpatient header - 6th position',
     u'Outpatient header - 7th position',
     u'Outpatient header - 8th position',
     u'Outpatient header - 9th position',
     u'Patient Self-Reported Condition',
     u'Primary Condition',
     u'Referral record',
     u'Secondary Condition']

    # procedure

    # observation - DRGs

    # drug_exposure


if __name__ == "__main__":
    with open("hi_config.json", "r") as f:
        config_dict = json.load(f)

    main(config_dict["csv_input_directory"], config_dict["csv_output_directory"], config_dict["json_map_directory"])



