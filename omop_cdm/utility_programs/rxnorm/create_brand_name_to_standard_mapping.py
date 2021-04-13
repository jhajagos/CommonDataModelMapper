import os

from multum_sourced_rxnorm_mappings import main as convert_csv_to_json


def main(directory):

    convert_csv_to_json([os.path.join(directory, "select_n_in__ot___from___select_bn_rxcui.csv")], key_field="bn_rxcui")
    convert_csv_to_json([os.path.join(directory, "select_tt_n_sbdf__ott___from___select_bn.csv")], key_field="bn_rxcui")

    convert_csv_to_json([os.path.join(directory, "select_n_in__ot___from___select_bn_rxcui.csv")], key_field="bn_str")
    convert_csv_to_json([os.path.join(directory, "select_tt_n_sbdf__ott___from___select_bn.csv")], key_field="bn_str")


if __name__ == "__main__":
    base_dir = "./"
    main(base_dir)




