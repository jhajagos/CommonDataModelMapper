import os
import sys
import argparse
import json

from utility_functions import load_csv_files_into_db, generate_db_dict


def main(output_directory, connection_string, schema):

    data_dict = generate_db_dict(output_directory)

    load_csv_files_into_db(connection_string, data_dict,schema_ddl=None, indices_ddl=None,
                           i_print_update=1000, truncate=True, schema=schema)


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser()
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file", default="../hi_config.json")

    arg_parse_obj.add_argument("--connection-uri", dest="connection_uri", default=None)
    arg_parse_obj.add_argument("--schema", dest="schema", default=None)

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

    main(config["csv_output_directory"], connection_uri, schema)

