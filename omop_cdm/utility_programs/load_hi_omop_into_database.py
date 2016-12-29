import os
import json
from utility_functions import load_csv_files_into_db, generate_db_dict


def main(output_directory, connection_string, schema):

    data_dict = generate_db_dict(output_directory)

    load_csv_files_into_db(connection_string, data_dict,schema_ddl=None, indices_ddl=None,
                           i_print_update=1000, truncate=True, schema=schema)

if __name__ == "__main__":

    with open("../hi_config.json") as f:
        config = json.load(f)

    main(config["csv_output_directory"], config["connection_uri"], schema=config["schema"])

