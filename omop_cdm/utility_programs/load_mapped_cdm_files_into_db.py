import argparse
import json
import sys
import os
import sys

try:
    from utility_functions import load_csv_files_into_db, generate_db_dict
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, os.path.pardir, "src")))
    from utility_functions import load_csv_files_into_db, generate_db_dict


def main(output_directory, connection_string, schema):

    data_dict = generate_db_dict(output_directory)

    load_csv_files_into_db(connection_string, data_dict, schema_ddl=None, indices_ddl=None,
                           i_print_update=10000, truncate=True, schema=schema, null_flag=True)


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
        db_schema = config["schema"]
    else:
        db_schema = arg_obj.schema

    main(config["csv_output_directory"], connection_uri, db_schema)