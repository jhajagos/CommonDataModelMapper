import os
import json

from multum_sourced_rxnorm_mappings import main as convert_csv_to_json
import argparse


def main(directory):

    convert_csv_to_json([os.path.join(directory, "select_n_in__ot___from___select_bn_rxcui.csv")], key_field="bn_rxcui")
    convert_csv_to_json([os.path.join(directory, "select_tt_n_sbdf__ott___from___select_bn.csv")], key_field="bn_rxcui")

    convert_csv_to_json([os.path.join(directory, "select_bn_single_in.csv")], key_field="RXCUI")

    convert_csv_to_json([os.path.join(directory, "select_n_in__ot___from___select_bn_rxcui.csv")], key_field="bn_str")
    convert_csv_to_json([os.path.join(directory, "select_tt_n_sbdf__ott___from___select_bn.csv")], key_field="bn_str")

    convert_csv_to_json([os.path.join(directory, "select_bn_single_in.csv")], key_field="STR")


if __name__ == "__main__":
    arg_parser_obj = argparse.ArgumentParser(description="Converts CSV files generated by 'generate_brand_name_to_csv_mappings.py'")
    arg_parser_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name", default="./rxnorm.json")

    arg_obj = arg_parser_obj.parse_args()
    config_json_file_name = arg_obj.config_json_file_name

    with open(config_json_file_name, "r") as f:
        config = json.load(f)

    destination_directory = config["json_map_directory"]

    main(destination_directory)




