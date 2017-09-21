import unittest
from source_to_cdm_functions import *


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

    def test_convert_dt_without_tz(self):

        ts1 = convert_datetime("2014-02-21 13:33:05")

        self.assertEquals("2014-02-21 13:33:05", ts1)

        ts2 = convert_datetime("2014-02-21 05:22")

        self.assertEquals("2014-02-21 05:22:00", ts2)

        ts3 = convert_datetime("2014-02-21")

        self.assertEqual("2014-02-21 00:00:00", ts3)

    def test_convert_date_time_to_unix_seconds(self):

        ts1 = {"datetime": "2014-02-21 13:33:05"}

        result_ts1 = MapDateTimeToUnixEpochSeconds().map(ts1)

        self.assertTrue("seconds_since_unix_epoch" in result_ts1)

        self.assertEqual(1392989585, result_ts1["seconds_since_unix_epoch"])


if __name__ == '__main__':
    unittest.main()
