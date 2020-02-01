from mapping_classes import MapperClass, InputClassCSVRealization, OutputClassCSVRealization, \
    build_input_output_mapper, RunMapperAgainstSingleInputRealization
import time
import csv
import os
import json
import logging
import re
import datetime
import sys


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
        key = list(date_dict.keys())[0]
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

    re_odbc_date = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$")
    re_odbc_date_time_1 = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{2}:[0-9]{2}$")
    re_odbc_date_time_2 = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{2}:[0-9]{2}:[0-9]{2}$")
    re_odbc_date_time_3 = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,4}$")

    if datetime_str == '':
        return null_date
    else:

        try:
            if re_odbc_date_time_1.match(datetime_str):
                localized_datetime = time.strptime(datetime_str, '%Y-%m-%d %H:%M')
            elif re_odbc_date.match(datetime_str):
                localized_datetime = time.strptime(datetime_str, '%Y-%m-%d')
            elif re_odbc_date_time_2.match(datetime_str):
                localized_datetime = time.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            elif re_odbc_date_time_3.match(datetime_str):
                localized_datetime = time.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                return null_date

        except(ValueError):
            return null_date

        return time.strftime('%Y-%m-%d %H:%M:%S', localized_datetime)


def create_json_map_from_csv_file(csv_file_name, lookup_field_name, lookup_value_field_name, json_file_name=None):

    if json_file_name is None:
        json_file_name = csv_file_name + ".json"

    if sys.version_info[0] == 2:
        with open(csv_file_name, "rb") as fc:
            dict_reader = csv.DictReader(fc)
            map_dict = {}

            for row_dict in dict_reader:
                map_key = row_dict[lookup_field_name]
                map_value = row_dict[lookup_value_field_name]
                map_dict[map_key] = {lookup_value_field_name: map_value}
    else:
        with open(csv_file_name, "r", newline="") as fc:
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
        datetime_value = input_dict[list(input_dict.keys())[0]]

        if "T" in datetime_value:  # Has a time zone embedded
            datetime_local = convert_datetime_with_tz(datetime_value)
        else:
            datetime_local = convert_datetime(datetime_value)

        date_part, time_part = datetime_local.split(" ")

        return {"date": date_part, "time": time_part}


class DateTimeWithTZ(MapperClass):

    def __init__(self, key=None):
        self.key = key

    def map(self, input_dict):

        if self.key is None:
            datetime_value = input_dict[list(input_dict.keys())[0]]
        else:
            if self.key in input_dict:
                datetime_value = input_dict[self.key]
            else:
                return {}

        if "T" in datetime_value:
            datetime_local = convert_datetime_with_tz(datetime_value)
        else:
            datetime_local = convert_datetime(datetime_value)

        return {"datetime": datetime_local}

class DateTimeWithTZDebug(MapperClass):

    def __init__(self, key=None):
        self.key = key

    def map(self, input_dict):

        if self.key is None:
            datetime_value = input_dict[list(input_dict.keys())[0]]
        else:
            if self.key in input_dict:
                datetime_value = input_dict[self.key]
            else:
                return {}

        print("'" + datetime_value + "'")

        if "T" in datetime_value:
            datetime_local = convert_datetime_with_tz(datetime_value)
        else:
            datetime_local = convert_datetime(datetime_value)

        print(datetime_local)
        raise RuntimeError

        return {"datetime": datetime_local}


class MapDateTimeToUnixEpochSeconds(MapperClass):

    def map(self, input_dict, field="datetime"):

        if field in input_dict and len(input_dict[field]) and input_dict[field] != "1900-01-01 00:00:00":
            date_value = input_dict[field]

            try:
                time_obj = datetime.datetime.strptime(date_value,
                                                      "%Y-%m-%d %H:%M:%S")  # Seconds since January 1, 1970 Unix time
            except ValueError:
                try:
                    time_obj = datetime.datetime.strptime(date_value,
                                                          "%Y-%m-%d")  # Seconds since January 1, 1970 Unix time
                except ValueError:
                    try:
                        time_obj = datetime.datetime.strptime(date_value, "%Y-%m-%d %H:%M")
                    except:
                        time_obj = datetime.datetime.strptime(date_value,
                                                      "%Y-%m-%d %H:%M:%S.%f")

            seconds_since_unix_epoch = (time_obj - datetime.datetime(1970, 1, 1)).total_seconds()

            return {"seconds_since_unix_epoch": seconds_since_unix_epoch}

        else:
            return {}


class FloatMapper(MapperClass):
    """Convert value to float"""

    def map(self, input_dict):
        resulting_map = {}
        for key in input_dict:
            try:
                resulting_map[key] = float(input_dict[key])
            except(ValueError, TypeError):
                if input_dict[key] in ("NULL", "None", "null"):
                    resulting_map[key] = ""

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


def generate_mapper_obj(input_csv_file_name, input_class_obj, output_csv_file_name, output_class_obj, map_rules_list,
                        output_obj, in_out_map_obj, input_router_func=None, pre_map_func=None, post_map_func=None):

    if input_router_func is None:
        input_router_func = lambda x: output_class_obj

    input_csv_class_obj = InputClassCSVRealization(input_csv_file_name, input_class_obj)
    output_csv_class_obj = OutputClassCSVRealization(output_csv_file_name, output_class_obj)

    map_rules_obj = build_input_output_mapper(map_rules_list)

    output_obj.register(output_class_obj, output_csv_class_obj)

    in_out_map_obj.register(input_class_obj, output_class_obj, map_rules_obj)

    map_runner_obj = RunMapperAgainstSingleInputRealization(input_csv_class_obj, in_out_map_obj, output_obj,
                                                            input_router_func, pre_map_func, post_map_func)

    return map_runner_obj


def register_to_mapper_obj(input_csv_file_name, input_class_obj, output_csv_file_name, output_class_obj,
                           map_rules_list,
                           output_obj, in_out_map_obj):

    input_csv_class_obj = InputClassCSVRealization(input_csv_file_name, input_class_obj)

    output_csv_class_obj = OutputClassCSVRealization(output_csv_file_name, output_class_obj)

    map_rules_obj = build_input_output_mapper(map_rules_list)

    output_obj.register(output_class_obj, output_csv_class_obj)

    in_out_map_obj.register(input_class_obj, output_class_obj, map_rules_obj)