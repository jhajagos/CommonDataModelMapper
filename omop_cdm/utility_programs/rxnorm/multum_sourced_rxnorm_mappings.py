import csv
import json
import os
import argparse

"""
This script requires exporting a set of tables from HealtheIntent EDW raw tables
to CSV. There a set of mappings to RxNorm which are not available through the 
RxNorm MULTUM source and are stored in Cerner Millennium Raw Tables.

The following tables are expected:

rxnorm_multum.csv
rxnorm_multum_drug.csv
rxnorm_multum_mmdc.csv

"""


def main(csv_files_list, key_field="MULDRUG_ID"):

    for csv_file in csv_files_list:

        with open(csv_file, "r", newline="") as f:
            keyed_dict = {}
            csv_dict_reader = csv.DictReader(f)
            for row_dict in csv_dict_reader:
                keyed_dict[row_dict[key_field]] = row_dict

            with open(csv_file + "." + key_field + ".json", "w") as fw:
                json.dump(keyed_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser(description="Get MULTUM RxNorm to Multum mappings")

    arg_parser_obj.add_argument("-d", "--base-healtheintent-export-directory", dest="base_directory")

    arg_obj = arg_parser_obj.parse_args()

    # These are based on raw table queries in HealtheIntent
    base_names = ["rxnorm_multum", "rxnorm_multum_drug", "rxnorm_multum_mmdc"]

    csv_files = []
    for base_name in base_names:
        csv_files += [os.path.join(arg_obj.base_directory, base_name + ".csv")]

    main(csv_files)
