import os
import json
from utility_functions import load_csv_files_into_db, generate_db_dict


def main(output_directory=None, vocabulary_directory=None, load_vocabularies=False, load_data=True):
    with open("../schema/omop_cdm.sql") as f:
        omop_cdm_sql = f.read()

    with open("../schema/omop_cdm_indexes.sql") as f:
        omop_cdm_idx_sql = f.read()

    output_sqlite3 = os.path.join(output_directory, "omop_db_load.db3")

    if os.path.exists(output_sqlite3):
        os.remove(output_sqlite3)

    data_dict = generate_db_dict(output_directory)

    connection_string = "sqlite:///" + output_sqlite3

    if not load_data:
        data_dict = {}

    load_csv_files_into_db(connection_string, data_dict, omop_cdm_sql, indices_ddl=omop_cdm_idx_sql,
                               i_print_update=1000)

    if load_vocabularies:
        vocab_pairs = generate_vocabulary_load(vocabulary_directory, vocabularies=["CONCEPT"])
        vocab_dict = {}
        for pair in vocab_pairs:
            vocab_dict[pair[1]] = pair[0]

        load_csv_files_into_db(connection_string, vocab_dict, delimiter="\t", i_print_update=100000)


def generate_vocabulary_load(vocabulary_directory,  vocabularies=["CONCEPT",
                    "CONCEPT_ANCESTOR",
                    "CONCEPT_CLASS",
                    "CONCEPT_RELATIONSHIP",
                    "CONCEPT_SYNONYM",
                    "DOMAIN",
                    "DRUG_STRENGTH",
                    "RELATIONSHIP",
                    "VOCABULARY"]):

    load_pairs = []
    for vocabulary in vocabularies:
        load_pairs += [(vocabulary.lower(), os.path.join(vocabulary_directory, vocabulary + ".csv"))]

    return load_pairs

if __name__ == "__main__":

    with open("../hi_config_mother_child.json") as f:
        config = json.load(f)

    main(output_directory=config["csv_output_directory"], vocabulary_directory=config["json_map_directory"], load_vocabularies=True)