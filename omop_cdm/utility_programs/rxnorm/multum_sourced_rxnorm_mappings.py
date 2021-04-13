import csv
import json


def main(csv_files_list, key_field="MULDRUG_ID"):

    for csv_file in csv_files_list:

        with open(csv_file, "r", newline="") as f:
            keyed_dict = {}
            csv_dict_reader = csv.DictReader(f)
            for row_dict in csv_dict_reader:
                keyed_dict[row_dict[key_field]] = row_dict

            with open(csv_file + "." + key_field + ".json", "w") as fw:
                json.dump(keyed_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))

