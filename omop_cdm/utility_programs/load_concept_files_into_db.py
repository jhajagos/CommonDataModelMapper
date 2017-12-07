import argparse
import json

from utility_functions import load_csv_files_into_db, generate_vocabulary_load


def main(vocab_directory, connection_string, schema, vocabularies=["CONCEPT"]):

    vocab_list = generate_vocabulary_load(vocab_directory, vocabularies)

    vocab_data_dict = {}
    for pair in vocab_list:
        vocab_data_dict[pair[1]] = pair[0]

    load_csv_files_into_db(connection_string, vocab_data_dict, schema_ddl=None, indices_ddl=None,
                           i_print_update=1000, truncate=True, schema=schema, delimiter="\t")


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Load concept/vocabulary files into database")
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file", default="../hi_config.json")

    arg_parse_obj.add_argument("--connection-uri", dest="connection_uri", default=None)
    arg_parse_obj.add_argument("--schema", dest="schema", default=None)

    arg_parse_obj.add_argument("--full-concept-files", default=False, action="store_true", dest="load_full_concept_files")

    arg_obj = arg_parse_obj.parse_args()

    print("Reading config file '%s'" % arg_obj.config_file_name)
    with open(arg_obj.config_file_name) as f:
        config = json.load(f)

    if arg_obj.connection_uri is None:
        connection_uri = config["connection_uri"]
    else:
        connection_uri = arg_obj.connection_uri

    if arg_obj.schema is None:
        schema = config["schema"]
    else:
        schema = arg_obj.schema

    if arg_obj.load_full_concept_files:
        vocabularies_to_load = ["CONCEPT", "CONCEPT_ANCESTOR", "CONCEPT_CLASS", "CONCEPT_RELATIONSHIP", "CONCEPT_SYNONYM",
                        "DOMAIN", "DRUG_STRENGTH", "RELATIONSHIP", "VOCABULARY"]

    else:
        vocabularies_to_load = ["CONCEPT"]

    main(config["json_map_directory"], connection_uri, schema, vocabularies=vocabularies_to_load)

