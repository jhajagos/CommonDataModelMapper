"""

Build JSON maps from a vocabulary to an RxNorm RXCUI

File layout of RXNCONSO.RRF file
[('2', 0), #RXCUI
 ('ENG', 1),
 ('', 2),
 ('', 3),
 ('', 4),
 ('', 5),
 ('', 6),
 ('3091093', 7), #RXAUI
 ('', 8),
 ('N0000007747', 9), #SCUI
 ('', 10),
 ('NDFRT', 11), #SAB
 ('PT', 12), #TTY
 ('N0000007747', 13), #CODE
 ('1,2-Dipalmitoylphosphatidylcholine', 14), #STR
 ('', 15),
 ('N', 16),
 ('', 17),
 ('', 18)]
"""

import csv
import json
import os
import argparse

RXNCONSO_FIELD_LAYOUT = {"RXCUI": 0, "LAT": 1, "RXAUI": 7, "SCUI": 9, "SAB": 11, "TTY": 12, "CODE": 13, "STR": 14, "SUPPRESS": 16}


def main(rrf_file_name, sab, tty, lookup_field, output_directory, field_layout=RXNCONSO_FIELD_LAYOUT):
    with open(rrf_file_name, newline="", encoding="utf8") as f:
        rrf_reader = csv.reader(f, delimiter="|")

        lookup_dict = {}
        i = 0
        for rrf_row in rrf_reader:
            rrf_dict = {}
            for key in field_layout:
                rrf_dict[key] = rrf_row[field_layout[key]]

            if rrf_dict["SAB"] == sab and rrf_dict["TTY"] == tty and rrf_dict["SUPPRESS"] == "N":
                lookup_dict[rrf_dict[lookup_field]] = rrf_dict

            i += 1

    output_json_file_name = os.path.join(output_directory, "RxNorm_" + sab + "_" + tty + ".json")

    with open(output_json_file_name, "w") as fw:
        json.dump(lookup_dict, fw, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == "__main__":

    arg_obj = argparse.ArgumentParser(description="Subset RxNorm RRF files for MMSL file")
    arg_obj.add_argument("-c", dest="config_file_name", help="JSON config file", default="./rxnorm.json")

    arg_parse_obj = arg_obj.parse_args()

    with open(arg_parse_obj.config_file_name, "r") as f:
        config = json.load(f)

    rxnorm_base_directory = config["rxnorm_base_directory"]
    json_map_directory = config["json_map_directory"]

    rxnorm_rrf_directory = os.path.join(rxnorm_base_directory, "rrf")

    main(os.path.join(rxnorm_rrf_directory, "RXNCONSO.RRF"), "MMSL", "BN", "CODE", json_map_directory)
    main(os.path.join(rxnorm_rrf_directory, "RXNCONSO.RRF"), "MMSL", "GN", "CODE", json_map_directory)
    main(os.path.join(rxnorm_rrf_directory, "RXNCONSO.RRF"), "MMSL", "CD", "CODE", json_map_directory)
    main(os.path.join(rxnorm_rrf_directory, "RXNCONSO.RRF"), "MMSL", "BD", "CODE", json_map_directory)