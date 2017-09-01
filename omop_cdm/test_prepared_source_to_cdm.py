import unittest
import sys
import json
import transform_prepared_source_to_cdm as tpsc

class TestMapping(unittest.TestCase):

    def setUp(self):
        with open("./test/test_config.json") as f:
            self.config = json.load(f)

    def test_something(self):

        tpsc.main("./test/input/", "./test/output/", self.config["json_map_directory"])


if __name__ == '__main__':
    unittest.main()
