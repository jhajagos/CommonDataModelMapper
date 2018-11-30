import argparse
import json
import os
import csv
import datetime
import hashlib
import logging
import sys

"""
This script maps data extracted from the HealthFacts database into 
a prepared source format. The prepared source format is then mapped
into the OHDSI format. 
"""

logging.basicConfig(level=logging.INFO)

try:
    from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
            CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper, ChainMapper, CascadeKeyMapper, \
            CascadeMapper, KeyTranslator, PassThroughFunctionMapper, CodeMapperDictClass, CodeMapperDictClass, ConstantMapper, \
            ReplacementMapper

    from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
        SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
        SourceProcedureObject, SourceMedicationObject

    from source_to_cdm_functions import generate_mapper_obj
    from hf_classes import HFPatient, HFCareSite, HFEncounter, HFObservationPeriod, HFDiagnosis, HFProcedure, HFResult, HFMedication
    from prepared_source_functions import build_name_lookup_csv, build_key_func_dict

except(ImportError):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, "src")))

    from mapping_classes import OutputClassCSVRealization, InputOutputMapperDirectory, OutputClassDirectory, \
        CoderMapperJSONClass, TransformMapper, FunctionMapper, FilterHasKeyValueMapper, ChainMapper, CascadeKeyMapper, \
        CascadeMapper, KeyTranslator, PassThroughFunctionMapper, CodeMapperDictClass, CodeMapperDictClass, \
        ConstantMapper, \
        ReplacementMapper

    from prepared_source_classes import SourcePersonObject, SourceCareSiteObject, SourceEncounterObject, \
        SourceObservationPeriodObject, SourceEncounterCoverageObject, SourceResultObject, SourceConditionObject, \
        SourceProcedureObject, SourceMedicationObject

    from source_to_cdm_functions import generate_mapper_obj
    from hf_classes import HFPatient, HFCareSite, HFEncounter, HFObservationPeriod, HFDiagnosis, HFProcedure, HFResult, \
        HFMedication
    from prepared_source_functions import build_name_lookup_csv, build_key_func_dict


def merge_lab_and_clinical_events_cvs(clinical_event_csv, lab_procedure_csv, out_result_csv, overwrite=True, sample_size=None):

    cross_mappings = [("patient_id", "patient_id", "patient_id"),
                      ("encounter_id", "encounter_id", "encounter_id"),
                      ('loinc_code', 'detail_lab_procedure_loinc_code', 'code'),
                      ('event_code_desc', 'detail_lab_procedure_lab_procedure_name', 'result_name'),
                      ('performed_dt_tm', 'lab_performed_dt_tm', 'performed_dt_tm'),
                      ('normal_high', 'normal_range_high', 'range_high'),
                      ('normal_low', 'normal_range_low', 'range_low'),
                      ('result_value_num', 'numeric_result', 'numeric_result'),
                      ('result_value_dt_tm', None, "date_result"),
                      ('result_unit','result_units_unit_display', 'result_unit'),
                      ('normalcy_desc','result_indicator_desc', 'result_indicator')]

    ce_field_map_dict ={}
    for cross_map in cross_mappings:
        if cross_map[0] is not None:
            ce_field_map_dict[cross_map[0]] = cross_map[2]

    lab_procedure_field_map_dict = {}
    for cross_map in cross_mappings:
        if cross_map[1] is not None:
            lab_procedure_field_map_dict[cross_map[1]] = cross_map[2]

    columns_to_map_to = [c[2] for c in cross_mappings]

    if overwrite:
        with open(out_result_csv, "w", newline="") as fw:
            csv_writer = csv.writer(fw)
            header = columns_to_map_to + ['source']
            csv_writer.writerow(header)

            field_maps_dict = [ce_field_map_dict, lab_procedure_field_map_dict]
            csv_file_list = [clinical_event_csv, lab_procedure_csv]
            field_type_list = ["clinical_event", "lab_procedure"]

            t = 0
            for field_map_dict in field_maps_dict:

                with open(csv_file_list[t], newline="") as f:
                    i = 0
                    csv_dict_reader = csv.DictReader(f)
                    for row_dict in csv_dict_reader:

                        row_to_write = [''] * len(header)

                        for field in field_map_dict:
                            if field in row_dict:
                                row_to_write[header.index(field_map_dict[field])] = row_dict[field]

                        row_to_write[-1] = field_type_list[t]

                        csv_writer.writerow(row_to_write)

                        if sample_size is not None:
                            if i == sample_size - 1:
                                break

                        i += 1
                t += 1


def generate_observation_period(encounter_csv_file_name, hf_period_observation_csv_file_name,
                                id_field_name, start_date_field_name, end_date_field_name):

    with open(encounter_csv_file_name, newline="") as f:
        dict_reader = csv.DictReader(f)
        observation_period_dict = {}

        for row_dict in dict_reader:

            start_date_value = row_dict[start_date_field_name]
            end_date_value = row_dict[end_date_field_name]

            if len(end_date_value) == 0:
                end_date_value = start_date_value

            id_value = row_dict[id_field_name]

            if id_value in observation_period_dict:
                past_start_date_value, past_end_date_value = observation_period_dict[id_value]

                if start_date_value < past_start_date_value:
                    set_start_date_value = start_date_value
                else:
                    set_start_date_value = past_start_date_value

                if end_date_value > past_end_date_value:
                    set_end_date_value = end_date_value
                else:
                    set_end_date_value = past_end_date_value

                observation_period_dict[id_value] = (set_start_date_value, set_end_date_value)

            else:
                observation_period_dict[id_value] = (start_date_value, end_date_value)

    with open(hf_period_observation_csv_file_name, "w", newline="") as fw:
        csv_writer = csv.writer(fw)

        csv_writer.writerow([id_field_name, start_date_field_name, end_date_field_name])

        for id_value in observation_period_dict:
            start_date_value, end_date_value = observation_period_dict[id_value]
            if start_date_value == "":
                start_date_value = end_date_value
            row_to_write = [id_value, start_date_value, end_date_value]
            csv_writer.writerow(row_to_write)


def generate_patient_csv_file(patient_encounter_csv_file_name, output_directory):
    """Create a patient CSV file from the encounter patient file"""

    patient_fields = ["marital_status", "patient_id", "race", "gender", "patient_sk"]
    
    file_to_write = os.path.join(output_directory, "hf_patient.csv")
    file_to_read = patient_encounter_csv_file_name

    with open(file_to_read, "r", newline="") as f:

        dict_reader = csv.DictReader(f)

        result_dict = {}
        for row_dict in dict_reader:

            admit_dt_tm_txt = row_dict["admitted_dt_tm"]
            if not len(admit_dt_tm_txt):
                admit_dt_tm_txt = row_dict["discharged_dt_tm"]

            if len(admit_dt_tm_txt):
                admit_dt_tm = datetime.datetime.strptime(admit_dt_tm_txt, "%Y-%m-%d %H:%M:%S")
                age_in_years = row_dict["age_in_years"]
                if len(age_in_years):
                    age_in_years_td = datetime.timedelta(int(float(age_in_years)) * 365.25)

                estimated_dob_dt_tm = admit_dt_tm - age_in_years_td
                year_of_birth = estimated_dob_dt_tm.year
            else:
                year_of_birth = None

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
                if year_of_birth is not None and existing_year_of_birth is not None:
                    if year_of_birth < existing_year_of_birth:
                        result_dict[patient_id] = patient_dict

        with open(file_to_write, "w", newline="") as fw:
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

    patient_file_name = generate_patient_csv_file(encounter_patient_file_name, input_csv_directory)

    file_name_dict["patient"] = patient_file_name
    print(file_name_dict)

    output_person_csv = os.path.join(output_csv_directory, "source_person.csv")

    output_class_obj = OutputClassDirectory()
    in_out_map_obj = InputOutputMapperDirectory()

    race_map = {
                "African American": "African American",
                "Asian": "Asian",
                #Biracial
                "Caucasian": "White",
                #Hispanic - ethnicity
                #Mid Eastern Indian
                "Native American": "American Indian or Alaska Native",
                #Not Mapped
                #NULL
                #Other
                "Pacific Islander": "Native Hawaiian or Other Pacific Islander"
                #Unknown
    }
    race_mapper = CodeMapperDictClass(race_map)

    ethnicity_source_map = {"Hispanic": "Hispanic"}
    ethnicity_source_mapper = CodeMapperDictClass(ethnicity_source_map)

    ethnicity_map = {"Hispanic": "Hispanic or Latino"}
    ethnicity_mapper = CodeMapperDictClass(ethnicity_map)

    hf_patient_rules = [("patient_id", "s_person_id"),
                        ("gender", "s_gender"),
                        ("gender", "m_gender"),
                        (("year_of_birth",), FunctionMapper(lambda x: x["year_of_birth"] + '-01-01', "date_of_birth"),
                        {"date_of_birth": "s_birth_datetime"}),
                        ("race", "s_race"),
                        ("race", race_mapper, {"mapped_value": "m_race"}),
                        ("race", ethnicity_source_mapper, {"mapped_value": "s_ethnicity"}),
                        ("race", ethnicity_mapper, {"mapped_value": "m_ethnicity"})
                        #,
                        #("year_of_birth", PassThroughFunctionMapper(no_year_of_birth), {"year_of_birth": "i_exclude"})
                        ]

    source_person_runner_obj = generate_mapper_obj(file_name_dict["patient"], HFPatient(), output_person_csv,
                                                   SourcePersonObject(), hf_patient_rules,
                                                   output_class_obj, in_out_map_obj)

    source_person_runner_obj.run()

    # Observation Period

    hf_observation_period_csv = os.path.join(input_csv_directory, "hf_observation_period.csv")
    generate_observation_period(encounter_file_name, hf_observation_period_csv,
                                "patient_id", "admitted_dt_tm", "discharged_dt_tm")

    observation_period_rules = [("patient_id", "s_person_id"),
                                ("admitted_dt_tm", "s_start_observation_datetime"),
                                ("discharged_dt_tm", "s_end_observation_datetime")]

    source_observation_period_csv = os.path.join(output_csv_directory, "source_observation_period.csv")

    observation_runner_obj = generate_mapper_obj(hf_observation_period_csv, HFObservationPeriod(),
                                                 source_observation_period_csv,
                                                 SourceObservationPeriodObject(), observation_period_rules,
                                                 output_class_obj, in_out_map_obj)
    observation_runner_obj.run()

    # Care site
    care_site_csv = os.path.join(input_csv_directory, "care_site.csv")

    md5_func = lambda x: hashlib.md5(x.encode("utf8")).hexdigest()

    key_care_site_mapper = build_name_lookup_csv(encounter_file_name, care_site_csv,
                                                 ["hospital_id", "caresetting_desc"],
                                                 ["hospital_id", "caresetting_desc"], hashing_func=md5_func)

    care_site_name_mapper = FunctionMapper(
        build_key_func_dict(["hospital_id", "caresetting_desc"], separator=" - "))

    care_site_rules = [("key_name", "k_care_site"),
                       (("hospital_id", "caresetting_desc"),
                         care_site_name_mapper,
                        {"mapped_value": "s_care_site_name"})]

    source_care_site_csv = os.path.join(output_csv_directory, "source_care_site.csv")

    care_site_runner_obj = generate_mapper_obj(care_site_csv, HFCareSite(), source_care_site_csv,
                                               SourceCareSiteObject(), care_site_rules,
                                               output_class_obj, in_out_map_obj)

    care_site_runner_obj.run()

    # Encounter

    ["s_encounter_id", "s_person_id", "s_visit_start_datetime", "s_visit_end_datetime", "s_visit_type",
     "m_visit_type", "k_care_site", "s_discharge_to", "m_discharge_to",
     "s_admitting_source", "m_admitting_source", "i_exclude"]

    def admit_discharge_mapper(dict_of_interest):
        admitted_dt_tm_txt = dict_of_interest["admitted_dt_tm"]
        discharged_dt_tm_txt = dict_of_interest["discharged_dt_tm"]

        if not len(admitted_dt_tm_txt):
            admitted_dt_tm_txt = discharged_dt_tm_txt

        if not len(discharged_dt_tm_txt):
            discharged_dt_tm_txt = admitted_dt_tm_txt

        return {"admitted_dt_tm": admitted_dt_tm_txt, "discharged_dt_tm": discharged_dt_tm_txt}



    encounter_rules = [
        ("encounter_id", "s_encounter_id"),
        ("patient_id", "s_person_id"),
        (("admitted_dt_tm", "discharged_dt_tm"),  PassThroughFunctionMapper(admit_discharge_mapper),
         {"admitted_dt_tm": "s_visit_start_datetime"}),
        (("admitted_dt_tm", "discharged_dt_tm"), PassThroughFunctionMapper(admit_discharge_mapper),
         {"discharged_dt_tm": "s_visit_end_datetime"}),
        ("patient_type_desc", "s_visit_type"),
        ("patient_type_desc", "m_visit_type"),
        (("hospital_id", "caresetting_desc"), key_care_site_mapper, {"mapped_value": "k_care_site"}),
        ("dischg_disp_code_desc", "s_discharge_to"),
        #("", "m_discharge_to"),
        ("admission_source_code_desc", "s_admitting_source")
        #("m_admitting_source")
    ]

    source_encounter_csv = os.path.join(output_csv_directory, "source_encounter.csv")

    encounter_runner_obj = generate_mapper_obj(encounter_file_name, HFEncounter(), source_encounter_csv,
                                               SourceEncounterObject(), encounter_rules,
                                               output_class_obj, in_out_map_obj)

    encounter_runner_obj.run()

    # Encounter plan or insurance coverage

    source_encounter_coverage_csv = os.path.join(output_csv_directory, "source_encounter_coverage.csv")

    encounter_coverage_rules = [("patient_id", "s_person_id"),
                                ("encounter_id", "s_encounter_id"),
                                ("admitted_dt_tm", "s_start_payer_date"),
                                ("discharged_dt_tm", "s_end_payer_date"),
                                ("payer_code_desc", "s_payer_name"),
                                ("payer_code_desc", "m_payer_name"),
                                ("payer_code_desc", "s_plan_name"),
                                ("payer_code_desc", "m_plan_name")]

    encounter_benefit_runner_obj = generate_mapper_obj(encounter_file_name,
                                                       HFEncounter(),
                                                       source_encounter_coverage_csv, SourceEncounterCoverageObject(),
                                                       encounter_coverage_rules, output_class_obj, in_out_map_obj)

    encounter_benefit_runner_obj.run()

    # Diagnosis / condition

    dx_code_oid_map = {
        "ICD9": "2.16.840.1.113883.6.103",
        "ICD10-CM": "2.16.840.1.113883.6.90"
    }

    dx_code_oid_mapper = CodeMapperDictClass(dx_code_oid_map)

    ["s_person_id", "s_encounter_id", "s_start_condition_datetime", "s_end_condition_datetime",
     "s_condition_code", "m_condition_code_oid", "s_sequence_id", "m_rank", "s_condition_type",
     "s_present_on_admission_indicator"]

    condition_rules = [("patient_id", "s_person_id"),
                       ("encounter_id", "s_encounter_id"),
                       ("diagnosis_code","s_condition_code"),
                       ("diagnosis_type", "s_condition_code_type"),
                       ("diagnosis_type", dx_code_oid_mapper, {"mapped_value": "m_condition_code_oid"}),
                       ("diagnosis_priority", "s_sequence_id"),
                       ("diagnosis_type_display", "s_condition_type"),
                       ("present_on_admit_code","s_present_on_admission_indicator"),
                       ("admitted_dt_tm", "s_start_condition_datetime"),
                       ("discharged_dt_tm", "s_end_condition_datetime")
                       ]

    hf_diagnosis_csv = os.path.join(input_csv_directory, file_name_dict["diagnosis"])
    source_condition_csv = os.path.join(output_csv_directory, "source_condition.csv")
    condition_mapper_obj = generate_mapper_obj(hf_diagnosis_csv, HFDiagnosis(), source_condition_csv,
                                               SourceConditionObject(),
                                               condition_rules, output_class_obj, in_out_map_obj)

    condition_mapper_obj.run()

    # Procedure

    """
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

    """

    procedure_code_oid_map = {
        "CPT4": "2.16.840.1.113883.6.12",
        "HCPCS": "2.16.840.1.113883.6.285",
        "ICD9": "2.16.840.1.113883.6.104",
        "ICD10-PCS": "2.16.840.1.113883.6.96"
    }

    procedure_code_oid_mapper = CodeMapperDictClass(procedure_code_oid_map)

    ["s_person_id", "s_encounter_id", "s_start_procedure_datetime", "s_end_procedure_datetime",
     "s_procedure_code", "s_procedure_code_type", "m_procedure_code_oid", "s_sequence_id", "s_rank", "m_rank"]

    hf_procedure_csv = os.path.join(input_csv_directory, file_name_dict["procedure"])
    source_procedure_csv = os.path.join(output_csv_directory, "source_procedure.csv")

    procedure_rules = [("patient_id", "s_person_id"),
                       ("encounter_id", "s_encounter_id"),
                       ("procedure_dt_tm", "s_start_procedure_datetime"),
                       ("procedure_code", "s_procedure_code"),
                       ("procedure_type", "s_procedure_code_type"),
                       ("procedure_type", procedure_code_oid_mapper, {"mapped_value":"m_procedure_code_oid"}),
                       #("", "s_rank"),
                       #("", "m_rank"),
                       ("procedure_priority","s_sequence_id")
                       ]

    procedure_mapper_obj = generate_mapper_obj(hf_procedure_csv, HFProcedure(), source_procedure_csv,
                                               SourceProcedureObject(),
                                               procedure_rules, output_class_obj, in_out_map_obj)

    procedure_mapper_obj.run()

    # Lab results and conditions
    # Merge the results and conditions together

    hf_result_csv = os.path.join(input_csv_directory, "hf_result.csv")
    source_result_csv = os.path.join(output_csv_directory, "source_result.csv")

    merge_lab_and_clinical_events_cvs(os.path.join(input_csv_directory, file_name_dict["clinical_events"]),
                                      os.path.join(input_csv_directory, file_name_dict["lab_procedure"]),
                                      hf_result_csv, overwrite=True, sample_size=None)

    result_map = {"LOINC": "2.16.840.1.113883.6.1", "SNOMED": "2.16.840.1.113883.6.285"}

    # Expand and validate this list
    clinical_event_name_snomed_code_map = {
        "Blood Pressure Diastolic": "271650006",
        "Blood Pressure Systolic": "271649006",
        "Respiratory Rate": "86290005",
        "SPO2 (Saturation of peripheral oxygen)": "59408-5",
        "Weight": "27113001",
        "Height": "50373000",
        #"Pulse Peripheral": "54718008",
        "Heart Rate": "8867-4",
        "Heart Rate: Monitored": "8867-4",
        "Mean Arterial Pressure": "8478-0",
        #"Level of Consciousness": "70184-7", # no numeric results
        "Glasgow Coma Score": "35088-4",
        "Temperature Skin": "39106-0",
        "Temperature (Route Not Specified)": "8310-5",
        "Pain Scale Score": "72514-3",
        "Pulse": "8867-4",
        "Pulse Peripheral": "8867-4",
        "O2 Saturation (SO2)": "59408-5",
        "Temperature Oral": "8310-5",
        "Braden Scale for Predicting Pressure Ulcer Risk": "81636-3",
        "Numeric Pain Scale 0-10": "72514-3",
        "FiO2 (Fraction of Inspired Oxygen)": "3150-0", # Need to validate this
        "Temperature Axillary": "8310-5",
        "Temperature Tympanic": "8310-5"
    }

    clinical_event_name_snomed_code_mapper = CodeMapperDictClass(clinical_event_name_snomed_code_map, "result_name")
    result_code_mapper = CascadeMapper(ChainMapper(FilterHasKeyValueMapper(["code"]), KeyTranslator({"code": "mapped_value"})),
                                       ChainMapper(FilterHasKeyValueMapper(["result_name"]), clinical_event_name_snomed_code_mapper))

    def func_result_code_type_mapper(input_dict):

        if "code" in input_dict:
            code_value = input_dict["code"]
            if "-" in code_value:
                return {"mapped_value": result_map["LOINC"]}
            elif "result_name" in input_dict:
                result_name = input_dict["result_name"]
                if result_name in clinical_event_name_snomed_code_map:
                    return {"mapped_value": result_map["SNOMED"]}

        return {"i_exclude": 1}

    def func_i_exclude_type_mapper(input_dict):

        result_dict = func_result_code_type_mapper(input_dict)
        if "i_exclude" in result_dict:
            return {"i_exclude": 1}
        else:
            if input_dict["numeric_result"]	in ("", "NULL") and input_dict["date_result"] in ("", "NULL"):
                return {"i_exclude": 1}
            else:
                return {}

    i_exclude_func_mapper = PassThroughFunctionMapper(func_i_exclude_type_mapper)

    code_type_mapper = PassThroughFunctionMapper(func_result_code_type_mapper)

    ["s_person_id", "s_encounter_id", "s_obtained_datetime", "s_type_name", "s_type_code", "m_type_code_oid",
     "s_result_text", "s_result_numeric", "s_result_datetime", "s_result_code", "m_result_code_oid",
     "s_result_unit", "s_result_unit_code", "m_result_unit_code_oid",
     "s_result_numeric_lower", "s_result_numeric_upper", "i_exclude"]
    # patient_id	encounter_id	code	result_name	performed_dt_tm	range_high	range_low	numeric_result	date_result	result_unit	result_indicator	source

    result_rules = [
        ("patient_id", "s_person_id"),
        ("encounter_id", "s_encounter_id"),
        ("performed_dt_tm", "s_obtained_datetime"),
        ("result_name", "s_type_name"),
        (("code", "result_name"), result_code_mapper, {"mapped_value": "s_type_code"}),
         (("code","result_name"), code_type_mapper, {"mapped_value": "m_type_code_oid"}),
        ("result_indicator", ReplacementMapper({"NULL": "", "Within Range": "Within reference range"}),
         {"result_indicator": "m_result_text"}),
        ("result_indicator", "s_result_text"),
        ("numeric_result", "s_result_numeric"),
        ("range_low", "s_result_numeric_lower"),
        ("range_high", "s_result_numeric_upper"),
        ("result_unit", "s_result_unit"),
        ("result_unit", ReplacementMapper({"NULL": ""}), {"result_unit": "m_result_unit"}), # TODO: Map to standard
        (("code","result_name", "numeric_result", "date_result"), i_exclude_func_mapper, {"i_exclude": "i_exclude"})
    ]

    result_mapper_obj = generate_mapper_obj(hf_result_csv, HFResult(), source_result_csv, SourceResultObject(),
                                            result_rules, output_class_obj, in_out_map_obj)

    result_mapper_obj.run()

    # Medication

    def zero_pad_ndc(input_dict):
        if "ndc_code" in input_dict:
            if len("ndc_code"):
                ndc_code = input_dict["ndc_code"]
                ndc_code_len = len(ndc_code)

                zeros_to_add = 11 - ndc_code_len

                input_dict["ndc_code"] = "0" * zeros_to_add + ndc_code

        return input_dict


    medication_rules = [
        ("patient_id", "s_person_id"),
        ("encounter_id", "s_encounter_id"),
        ("ndc_code", PassThroughFunctionMapper(zero_pad_ndc), {"ndc_code": "s_drug_code"}),
        ("ndc_code", ConstantMapper({"mapped_value": "2.16.840.1.113883.6.69"}), {"mapped_value": "m_drug_code_oid"}),
        (":row_id", ConstantMapper({"s_drug_code_type": "NDC"}), {"s_drug_code_type": "s_drug_code_type"}),
        ("brand_name", "s_drug_alternative_text"),
        ("generic_name", "s_drug_text"),
        ("med_started_dt_tm", "s_start_medication_datetime"),
        ("med_stopped_dt_tm", "s_end_medication_datetime"),
        ("dose_form_description", "s_dose"),
        ("dose_form_description", "m_dose"),
        ("route_description", "s_route"),
        ("route_description", "m_route"),
        ("total_dispensed_doses","s_quantity"),
        ("order_strength_units_unit_display", "s_dose_unit"),
        ("order_strength_units_unit_display", "m_dose_unit"),
        ("med_order_status_desc", "s_status"),
        (":row_id", ConstantMapper({"m_drug_type": "HOSPITAL_PHARMACY"}), {"m_drug_type": "m_drug_type"})
    ]

    hf_medication_csv = os.path.join(input_csv_directory, file_name_dict["medication"])
    source_medication_csv = os.path.join(output_csv_directory, "source_medication.csv")

    medication_mapper_obj = generate_mapper_obj(hf_medication_csv, HFMedication(), source_medication_csv,
                                                SourceMedicationObject(), medication_rules,
                                                output_class_obj, in_out_map_obj)

    medication_mapper_obj.run()


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser()
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="hf_config.json")
    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name, "r") as f:
        config_dict = json.load(f)

    file_name_dict = {
        "clinical_events": "clinical_event_joined_to_export.csv",
        "diagnosis": "diagnosis_joined_to_export.csv",
        "encounter": "encounter_joined_to_export.csv",
        "encounter_patient": "encounter_joined_to_export.csv",
        "lab_procedure": "lab_procedure_joined_to_export.csv",
        "medication": "medication_joined_to_export.csv",
        "procedure": "procedure_joined_to_export.csv"
    }

    main(config_dict["csv_input_directory"], config_dict["csv_input_directory"], file_name_dict)
