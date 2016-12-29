import os
import json
from utility_functions import load_csv_files_into_db, generate_db_dict


def main(output_directory, connection_string):

    with open("../schema/omop_cdm.sql") as f:
        omop_cdm_sql = f.read()

    with open("../schema/omop_cdm_indexes.sql") as f:
        omop_cdm_idx_sql = f.read()

    data_dict = generate_db_dict(output_directory)

    load_csv_files_into_db(connection_string, data_dict, omop_cdm_sql, indices_ddl=omop_cdm_idx_sql,
                           i_print_update=1000)

if __name__ == "__main__":


    with open("../hi_config.json") as f:
        config = json.load(f)


    main(config["csv_output_directory"], config["db_connection_uri"])

