"""
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

RXNCONSO_FIELD_LAYOUT = {"RXCUI": 0, "LAT": 1, "RXAUI": 7, "SCUI": 9, "SAB": 11, "TTY": 12, "CODE": 13, "STR": 14, "SUPPRESS": 16}


def main(rrf_file_name, sab, tty, lookup_field,output_directory, field_layout=RXNCONSO_FIELD_LAYOUT):
    with open(rrf_file_name, "r") as f:
        rrf_reader = csv.reader(f, delimiter="|")

        lookup_dict = {}
        for rrf_row in rrf_reader:
            rrf_dict = {}
            for key in field_layout:
                rrf_dict[key] = rrf_row[field_layout[key]]

            if rrf_dict["SAB"] == sab and rrf_dict["TTY"] == tty and rrf_dict["SUPPRESS"] == "N":
                lookup_dict[rrf_dict[lookup_field]] = rrf_dict

    output_json_file_name = os.path.join(output_directory, "RxNorm_" + sab + "_" + tty + ".json")

    with open(output_json_file_name, "w") as fw:
        json.dump(lookup_dict, fw, sort_keys = True, indent = 4, separators = (',', ': '))


if __name__ == "__main__":
    main("E:\\data\\RxNorm_full_09062016\\rrf\\RXNCONSO.RRF", "MMSL", "BN", "CODE", "E:\\data\\RxNorm_full_09062016\\rrf\\")
    main("E:\\data\\RxNorm_full_09062016\\rrf\\RXNCONSO.RRF", "MMSL", "GN", "CODE",
         "E:\\data\\RxNorm_full_09062016\\rrf\\")
    main("E:\\data\\RxNorm_full_09062016\\rrf\\RXNCONSO.RRF", "MMSL", "CD", "CODE",
         "E:\\data\\RxNorm_full_09062016\\rrf\\")
    main("E:\\data\\RxNorm_full_09062016\\rrf\\RXNCONSO.RRF", "MMSL", "BD", "CODE",
         "E:\\data\\RxNorm_full_09062016\\rrf\\")


