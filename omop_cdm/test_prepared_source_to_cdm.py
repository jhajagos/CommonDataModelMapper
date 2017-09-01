import unittest
import json
import transform_prepared_source_to_cdm as tpsc
import csv
import os


class TestMapping(unittest.TestCase):

    def setUp(self):
        with open("./test/test_config.json") as f:
            self.config = json.load(f)

        files_to_clean = ["person_cdm.csv", "death_cdm.csv"]
        for file_to_clean in files_to_clean:
            full_file_name = os.path.join("./test/output/", file_to_clean)
            if os.path.exists(full_file_name):
                os.remove(full_file_name)

    def test_something(self):

        tpsc.main("./test/input/", "./test/output/", self.config["json_map_directory"])

        with open("./test/output/person_cdm.csv") as f:
            csv_dict_reader = csv.DictReader(f)
            results_person = list(csv_dict_reader)
            self.assertEquals(4, len(results_person))

        with open("./test/output/death_cdm.csv") as f:
            csv_dict_reader = csv.DictReader(f)
            results_death = list(csv_dict_reader)
            self.assertEquals(1, len(results_death))


if __name__ == '__main__':
    unittest.main()