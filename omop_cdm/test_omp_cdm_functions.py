import unittest
from omop_cdm_functions import *

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


if __name__ == '__main__':
    unittest.main()
