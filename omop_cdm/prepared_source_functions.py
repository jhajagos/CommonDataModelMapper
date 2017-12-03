import csv
from mapping_classes import FunctionMapper
import sys


def build_key_func_dict(fields, hashing_func=None, separator="|"):
    if fields.__class__ not in ([].__class__, ().__class__):
        fields = [fields]

    def hash_func(input_dict):
        key_list = []
        for field in fields:
            key_list += [input_dict[field]]

        key_list = [kl for kl in key_list if len(kl)]
        key_string = separator.join(key_list)

        if hashing_func is not None:
            key_string = hashing_func(key_string)

        return key_string

    return hash_func


def build_name_lookup_csv(input_csv_file_name, output_csv_file_name, field_names, key_fields, hashing_func=None):
    """"""

    lookup_dict = {}
    key_func = build_key_func_dict(key_fields, hashing_func=hashing_func)

    if sys.version_info[0] == 2:
        with open(input_csv_file_name, "rb") as f:
            csv_dict = csv.DictReader(f)

            for row_dict in csv_dict:
                key_str = key_func(row_dict)
                new_dict = {}
                for field_name in field_names:
                    new_dict[field_name] = row_dict[field_name]

                lookup_dict[key_str] = new_dict
    else:
        with open(input_csv_file_name, "r", newline="") as f:
            csv_dict = csv.DictReader(f)

            for row_dict in csv_dict:
                key_str = key_func(row_dict)
                new_dict = {}
                for field_name in field_names:
                    new_dict[field_name] = row_dict[field_name]

                lookup_dict[key_str] = new_dict

    if sys.version_info[0] == 2:
        with open(output_csv_file_name, "wb") as fw:
            csv_writer = csv.writer(fw)

            i = 0
            for key_name in lookup_dict:

                row_dict = lookup_dict[key_name]
                if i == 0:
                    row_field_names = row_dict.keys()
                    header = ["key_name"] + row_field_names

                    csv_writer.writerow(header)

                if len(key_name):
                    row_to_write = [key_name]
                    for field_name in row_field_names:
                        row_to_write += [row_dict[field_name]]

                    csv_writer.writerow(row_to_write)
                i += 1
    else:
        with open(output_csv_file_name, "w", newline="") as fw:
            csv_writer = csv.writer(fw)

            i = 0
            for key_name in lookup_dict:

                row_dict = lookup_dict[key_name]
                if i == 0:
                    row_field_names = list(row_dict.keys())
                    header = ["key_name"] + row_field_names

                    csv_writer.writerow(header)

                if len(key_name):
                    row_to_write = [key_name]
                    for field_name in row_field_names:
                        row_to_write += [row_dict[field_name]]

                    csv_writer.writerow(row_to_write)
                i += 1

    return FunctionMapper(key_func)
