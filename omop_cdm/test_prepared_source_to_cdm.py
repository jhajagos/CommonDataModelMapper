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

        result_payer_plan_period = read_csv_file_as_dict("./test/output/payer_plan_period_cdm.csv")
        self.assertEquals(1, len(result_payer_plan_period))

        result_measurement = read_csv_file_as_dict("./test/output/measurement_encounter_cdm.csv")
        self.assertTrue(len(result_measurement))

        result_observation = read_csv_file_as_dict("./test/output/observation_measurement_encounter_cdm.csv")
        self.assertTrue(len(result_observation) == 0)

        result_condition = read_csv_file_as_dict("./test/output/condition_occurrence_dx_cdm.csv")
        self.assertTrue(len(result_condition))
        # TODO: Add conditions that map to other domains

        result_procedure = read_csv_file_as_dict("./test/output/procedure_cdm.csv")
        self.assertTrue(len(result_procedure))
        # TODO: Add procedures that map to other domains

        result_drug_exposure = read_csv_file_as_dict("./test/output/drug_exposure_cdm.csv")
        self.assertTrue(len(result_drug_exposure))


if __name__ == '__main__':
    unittest.main()
