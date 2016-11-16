import csv
import json
import os

def main(csv_files_list, key_field="MULDRUG_ID"):

    for csv_file in csv_files_list:

        with open(csv_file, "rb") as f:
            keyed_dict = {}
            csv_dict_reader = csv.DictReader(f)
            for row_dict in csv_dict_reader:
                keyed_dict[row_dict[key_field]] = row_dict

            with open(csv_file + ".json", "w") as fw:
                json.dump(keyed_dict, fw, sort_keys = True, indent=4, separators=(',', ': '))


if __name__ == "__main__":
    base_directory = "E:\\data\\rxnorm_multum\\"

    base_names = ["rxnorm_multum", "rxnorm_multum_drug", "rxnorm_multum_mmdc"]

    csv_files = []
    for base_name in base_names:
        csv_files += [os.path.join(base_directory, base_name + ".csv")]

    print(csv_files)

    main(csv_files)