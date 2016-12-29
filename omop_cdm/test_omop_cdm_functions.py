import unittest
from omop_cdm_functions import *
import json

from hi_etl_map_to_cdm import generate_rxcui_drug_code_mapper, generate_drug_name_mapper
import mapping_classes as mc

class TestMappers(unittest.TestCase):
    def test_date_split(self):

        dsm = DateSplit()
        dsm_result = dsm.map({"dob": "2012-01-02"})

        self.assertEquals(2012, dsm_result["year"])
        self.assertEquals(1, dsm_result["month"])
        self.assertEquals(2, dsm_result["day"])


class TestUtilityFunctions(unittest.TestCase):

    def test_convert_dt_with_tz(self):

        ts1 = convert_datetime_with_tz("2014-02-21T19:33:00-06:00")
        self.assertEquals("2014-02-21 13:33:00", ts1)

        ts2 = convert_datetime_with_tz("2014-02-21T01:33:00-02:00")
        self.assertEquals("2014-02-20 23:33:00", ts2)


class TestDrugCodeMapper(unittest.TestCase):

    def setUp(self):

        with open("./hi_config.json") as f:
            config = json.load(f)
        self.load_json_directory = config["json_map_directory"]

    def test_d_code(self):
        drug_code_mapper = generate_rxcui_drug_code_mapper(self.load_json_directory)

        dict_to_map_1 = {"drug_raw_code": "d00313", "drug_raw_coding_system_id": "2.16.840.1.113883.6.314"}

        mapping_result_1 = drug_code_mapper.map(dict_to_map_1)
        print(mapping_result_1)
        self.assertTrue(len(mapping_result_1))

        dict_to_map_2 = {"drug_raw_code": "d03431", "drug_raw_coding_system_id": "2.16.840.1.113883.6.314"}

        mapping_result_2 = drug_code_mapper.map(dict_to_map_2)

        #self.assertFalse(len(mapping_result_2))

        rxnorm_code_mapper_json = os.path.join(self.load_json_directory, "CONCEPT_CODE_RxNorm.json")

        rxnorm_code_mapper = mc.CoderMapperJSONClass(rxnorm_code_mapper_json, "RXNORM_ID")

        rxnorm_code_mapper_concept = mc.ChainMapper(drug_code_mapper, rxnorm_code_mapper)

        mapping_result3 = rxnorm_code_mapper_concept.map(dict_to_map_1)

        print(mapping_result3)

        self.assertTrue(0)

    def test_drug_name_mapper(self):

        drug_name_mapper = generate_drug_name_mapper(self.load_json_directory)
        dict_to_map_1 = {"drug_primary_display": "Toprol-XL"}
        print(drug_name_mapper.map(dict_to_map_1))

        raise





if __name__ == '__main__':
    unittest.main()
