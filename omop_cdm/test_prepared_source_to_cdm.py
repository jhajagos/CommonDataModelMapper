import unittest
import json
import transform_prepared_source_to_cdm as tpsc
import csv
import os


def read_csv_file_as_dict(csv_file_name):
    with open(csv_file_name) as f:
        return list(csv.DictReader(f))


class TestMapping(unittest.TestCase):

    def setUp(self):
        with open("./test/test_config.json") as f:
            self.config = json.load(f)

        files_to_clean = ["person_cdm.csv", "person_cdm.csv", "death_cdm.csv", "observation_period_cdm.csv",
                          "visit_occurrence_cdm.csv", "visit_occurrence_cdm.csv.json",
                          "measurement_encounter_cdm.csv", "observation_measurement_encounter_cdm.csv",
                          "condition_occurrence_dx_cdm.csv", "measurement_dx_cdm.csv", "observation_dx_cdm.csv",
                          "procedure_dx_cdm.csv",
                          "procedure_cdm.csv", "drug_exposure_proc_cdm.csv", "measurement_proc_cdm.csv",
                          "observation_proc_cdm.csv", "device_exposure_proc_cdm.csv", "drug_exposure_cdm.csv"
                          ]

        for file_to_clean in files_to_clean:
            full_file_name = os.path.join("./test/output/", file_to_clean)
            if os.path.exists(full_file_name):
                os.remove(full_file_name)

    def test_cdm_file_generation(self):

        tpsc.main("./test/input/", "./test/output/", self.config["json_map_directory"])

        results_person = read_csv_file_as_dict("./test/output/person_cdm.csv")
        self.assertEquals(4, len(results_person))

        first_person = results_person[0]

        self.assertNotEquals("", first_person["birth_datetime"])

        results_death = read_csv_file_as_dict("./test/output/death_cdm.csv")
        self.assertEquals(1, len(results_death))

        # TODO: Add different visit types inpatient, outpatient, and ED
        # TODO: Add support for 5.2 added datetime
        results_observation_period = read_csv_file_as_dict("./test/output/observation_period_cdm.csv")
        self.assertEquals(4, len(results_observation_period))

        results_visit_occurrence = read_csv_file_as_dict("./test/output/visit_occurrence_cdm.csv")
        self.assertEquals(1, len(results_visit_occurrence))

        first_visit = results_visit_occurrence[0]

        self.assertEqual("1", first_visit["care_site_id"])

        self.assertNotEquals("", first_visit["visit_start_datetime"])
        self.assertNotEquals("", first_visit["visit_end_datetime"])

        self.assertNotEquals("", first_visit["admitting_source_concept_id"])
        self.assertNotEquals("", first_visit["discharge_to_concept_id"])

        result_payer_plan_period = read_csv_file_as_dict("./test/output/payer_plan_period_cdm.csv")
        self.assertEquals(1, len(result_payer_plan_period))

        result_measurement = read_csv_file_as_dict("./test/output/measurement_encounter_cdm.csv")
        self.assertTrue(len(result_measurement))

        first_measurement = result_measurement[0]
        self.assertTrue(len(first_measurement["visit_occurrence_id"])) # Has a mapped visit_occurrence_id
        self.assertNotEquals("", first_measurement["measurement_datetime"])

        result_observation = read_csv_file_as_dict("./test/output/observation_measurement_encounter_cdm.csv")
        self.assertTrue(len(result_observation))

        first_observation = result_observation[0]
        self.assertNotEqual("", first_observation["observation_datetime"])

        result_condition = read_csv_file_as_dict("./test/output/condition_occurrence_dx_cdm.csv")
        self.assertTrue(len(result_condition))
        # TODO: Add conditions that map to other domains

        self.assertNotEquals("", result_condition[0]["condition_start_datetime"])

        result_procedure = read_csv_file_as_dict("./test/output/procedure_cdm.csv")
        self.assertTrue(len(result_procedure))
        # TODO: Add procedures that map to other domains

        self.assertNotEquals("", result_procedure[0]["procedure_datetime"])

        result_drug_exposure = read_csv_file_as_dict("./test/output/drug_exposure_cdm.csv")
        self.assertTrue(len(result_drug_exposure))

        first_drug_exposure = result_drug_exposure[0]
        self.assertNotEquals("0", first_drug_exposure["drug_concept_id"])


class TestCodeMappers(unittest.TestCase):
    def setUp(self):

        with open("./test/test_config.json") as f:
            self.config = json.load(f)

    def test_rxnorm_code_mapper(self):
        rxcui_mapper = tpsc.generate_rxcui_drug_code_mapper(self.config["json_map_directory"])

        input_dict_1 = {"s_drug_text": "Ciptodex", "s_drug_code": "00065853302",
                        "m_drug_code_oid": "2.16.840.1.113883.6.69"}

        output_dict_1 = rxcui_mapper.map(input_dict_1)
        #self.assertTrue("RXNORM_ID" in output_dict_1)

        input_dict_2 = {"s_drug_text": "Abilify", "s_drug_code": "352393", "m_drug_code_oid": "2.16.840.1.113883.6.88"}

        output_dict_2 = rxcui_mapper.map(input_dict_2)

        self.assertTrue("RXNORM_ID" in output_dict_2)

    def test_rxnorm_name_mapper(self):
        rxnorm_name_mapper = tpsc.generate_drug_name_mapper(self.config["json_map_directory"])

        input_dict_1 = {"s_drug_text": "Abilify"}

        output_dict_1 = rxnorm_name_mapper.map(input_dict_1)

        self.assertTrue("CONCEPT_ID" in output_dict_1)

    def test_d_code(self):
        drug_code_mapper = tpsc.generate_rxcui_drug_code_mapper(self.config["json_map_directory"])

        dict_to_map_1 = {"s_drug_code": "d00313", "m_drug_code_oid": "2.16.840.1.113883.6.314"}

        mapping_result_1 = drug_code_mapper.map(dict_to_map_1)
        #print(mapping_result_1)
        self.assertTrue(len(mapping_result_1))

        dict_to_map_2 = {"s_drug_code": "d03431", "m_drug_code_oid": "2.16.840.1.113883.6.314"}

        mapping_result_2 = drug_code_mapper.map(dict_to_map_2)

        self.assertTrue(len(mapping_result_2))

        rxnorm_code_mapper_json = os.path.join(self.config["json_map_directory"], "CONCEPT_CODE_RxNorm.json")

        rxnorm_code_mapper = tpsc.CoderMapperJSONClass(rxnorm_code_mapper_json, "RXNORM_ID")

        rxnorm_code_mapper_concept = tpsc.ChainMapper(drug_code_mapper, rxnorm_code_mapper)

        mapping_result3 = rxnorm_code_mapper_concept.map(dict_to_map_1)

        #print(mapping_result3)

        self.assertTrue('S', mapping_result3["STANDARD_CONCEPT"])


if __name__ == '__main__':
    unittest.main()
