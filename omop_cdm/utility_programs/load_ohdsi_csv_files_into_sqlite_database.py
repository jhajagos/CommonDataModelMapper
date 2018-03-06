import os
import json
import argparse
import sys

try:
    from utility_functions import load_csv_files_into_db, generate_db_dict, generate_vocabulary_load
except ImportError:
    sys.path.insert(0, os.path.join(os.path.pardir, os.path.pardir, "src"))
    from utility_functions import load_csv_files_into_db, generate_db_dict, generate_vocabulary_load


def main(file_table_list=None, sqlite_file_name=None, vocabulary_directory=None, load_vocabularies=False, load_data=True,
         schema_v="5.2", load_full_vocabularies=False, load_directory=None):

    with open("../schema/" + schema_v + "/omop_cdm.sql") as f:
        omop_cdm_sql = f.read()

    with open("../schema/" + schema_v + "/omop_cdm_indexes.sql") as f:
        omop_cdm_idx_sql = f.read()

    if os.path.exists(sqlite_file_name):
        os.remove(sqlite_file_name)

    if load_directory is not None:
        data_dict = generate_db_dict(load_directory)

    connection_string = "sqlite:///" + sqlite_file_name

    if not load_data:
        data_dict = {}

    if file_table_list is not None:
        data_dict = generate_db_dict(load_pairs=file_table_list)

    if file_table_list is None and load_directory is None:
        data_dict = {}

    load_csv_files_into_db(connection_string, data_dict, omop_cdm_sql, indices_ddl=omop_cdm_idx_sql,
                               i_print_update=1000)

    if load_vocabularies:

        if not load_full_vocabularies:
            vocabulary_tables = ["CONCEPT"]
        else:
            vocabulary_tables = ["CONCEPT", "CONCEPT_ANCESTOR", "CONCEPT_CLASS", "CONCEPT_RELATIONSHIP", "CONCEPT_SYNONYM",
                        "DOMAIN", "DRUG_STRENGTH", "RELATIONSHIP", "VOCABULARY"]

        vocab_pairs = generate_vocabulary_load(vocabulary_directory, vocabularies=vocabulary_tables)

        vocab_dict = {}
        for pair in vocab_pairs:
            vocab_dict[pair[1]] = pair[0]

        load_csv_files_into_db(connection_string, vocab_dict, delimiter="\t", i_print_update=100000)


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Load CSV files into a sqlite database with OHDSI")
    arg_parse_obj.add_argument("-c", "--config-json-file-name", dest="config_json_file_name",
                               default="cdm_config.json")
    arg_parse_obj.add_argument("-j", "--json-load-table-file-name", dest="json_load_table_file_name", default=None)
    arg_parse_obj.add_argument("-l", "--load-vocabulary", dest="load_vocabulary", default=False, action="store_true")
    arg_parse_obj.add_argument("-d", "--database-file-name", dest="database_file_name", default="ohdsi_cdm.db3")
    arg_parse_obj.add_argument("-v", "--load-full-vocabulary", dest="load_full_vocabulary", default=False,
                               action="store_true")
    arg_parse_obj.add_argument("-m", "--mapped-files-directory", dest="mapped_files_directory", default=None)

    arg_obj = arg_parse_obj.parse_args()

    with open(arg_obj.config_json_file_name) as f:
        config = json.load(f)

    if arg_obj.json_load_table_file_name is not None:
        with open(arg_obj.json_load_table_file_name)  as f:
            load_tables = json.load(f)
    else:
        load_tables = None

        """        
def main(file_table_list=None, sqlite_file_name=None, vocabulary_directory=None, load_vocabularies=False, load_data=,
         schema_v="5.2", load_full_vocabularies=False, load_directory=None):
        """
    main(load_tables, arg_obj.database_file_name, vocabulary_directory=config["json_map_directory"],
         load_vocabularies=arg_obj.load_vocabulary, load_data=True,
         load_full_vocabularies=arg_obj.load_full_vocabulary, load_directory=arg_obj.mapped_files_directory)
