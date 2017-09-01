from mapping_classes import MapperClass
import time
import csv
import os
import json
import logging


class LeftMapperString(MapperClass):
    def __init__(self, length):
        self.length = length

    def map(self, input_dict):
        new_dict = {}
        for key in input_dict:
            new_dict[key] = input_dict[key][0:self.length]

        return new_dict


class DateSplit(MapperClass):
    """Split a date"""
    def map(self, date_dict):
        key = date_dict.keys()[0]
        date_string = date_dict[key]

        try:
            year, month, day = date_string.split("-")
        except:
            return {"year": None, "month": None, "day": None}
        try:
            int_year = int(year)
        except ValueError:
            int_year = None

        try:
            int_month = int(month)
        except ValueError:
            int_month = None

        try:
            int_day = int(day)
        except ValueError:
            int_day = None

        return {"year": int_year, "month": int_month, "day": int_day}


def convert_datetime_with_tz(datetime_tz):
    null_date = "1900-01-01 00:00:00"
    if datetime_tz == '':
        return null_date

    dt_dts = datetime_tz[:-6]
    dt_h_string = datetime_tz[-6:-3]
    dt_h = int(dt_h_string)
    dt_dts_tuple = time.strptime(dt_dts, "%Y-%m-%dT%H:%M:%S")
    try:
        dt_dts_epoch = time.mktime(dt_dts_tuple) + dt_h * 60.0 * 60
    except: # ValueError, time.OverflowError
        logging.error("Invalid date '%s'" % datetime_tz)
        return null_date
    localized_datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(dt_dts_epoch))
    return localized_datetime


def convert_datetime(datetime_str):
    null_date = "1900-01-01 00:00:00"
    if datetime_str == '':
        return null_date
    else:
        if " " in datetime_str:
            localized_datetime = time.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        else:
            localized_datetime = time.strptime(datetime_str, '%Y-%m-%d')

        return time.strftime('%Y-%m-%d %H:%M:%S', localized_datetime)


        return datetime_str

def create_json_map_from_csv_file(csv_file_name, lookup_field_name, lookup_value_field_name, json_file_name=None):

    if json_file_name is None:
        json_file_name = csv_file_name + ".json"

    with open(csv_file_name, "rb") as fc:
        dict_reader = csv.DictReader(fc)
        map_dict = {}

        for row_dict in dict_reader:
            map_key = row_dict[lookup_field_name]
            map_value = row_dict[lookup_value_field_name]
            map_dict[map_key] = {lookup_value_field_name: map_value}

    with open(json_file_name, "w") as fwj:
        json.dump(map_dict, fwj)

    return os.path.abspath(json_file_name)


class SplitDateTimeWithTZ(MapperClass):
    """Split datetime into two parts and convert time to local time"""
    def map(self, input_dict):
        datetime_value = input_dict[input_dict.keys()[0]]

        if "T" in datetime_value:
            datetime_local = convert_datetime_with_tz(datetime_value)
        else:
            datetime_local = convert_datetime(datetime_value)

        date_part, time_part = datetime_local.split(" ")

        return {"date": date_part, "time": time_part}


class DateTimeWithTZ(MapperClass):

    def map(self, input_dict):
        datetime_value = input_dict[input_dict.keys()[0]]
        if "T" in datetime_value:
            datetime_local = convert_datetime_with_tz(datetime_value)
        else:
            datetime_local = convert_datetime(datetime_value)

        return {"datetime_local": datetime_local}


class FloatMapper(MapperClass):

    def map(self, input_dict):
        resulting_map = {}
        for key in input_dict:
            try:
                resulting_map[key] = float(input_dict[key])
            except ValueError:
                pass

        return resulting_map


class row_map_offset(MapperClass):

    def __init__(self, field_name, start_i=0):
        self.start_i = start_i
        self.field_name = field_name

    def map(self, input_dict):
        return {self.field_name: input_dict[":row_id"] + self.start_i}


def get_largest_id_from_csv_file(csv_file_name, primary_key_field_name):

    max_value = 0
    with open(csv_file_name, "rb") as f:
        cdict = csv.DictReader(f)
        for row_dict in cdict:
            max_value = max(max_value, int(row_dict[primary_key_field_name]))

    return max_value


def capitalize_words_and_normalize_spacing(input_string):
    split_input_string = input_string.split()
    capitalized_input_string = ""
    for token in split_input_string:
        capitalized_input_string += token[0].upper() + token[1:].lower() + " "
    return capitalized_input_string.strip()
