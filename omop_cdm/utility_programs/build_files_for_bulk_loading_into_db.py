"""
Combine multiple files and optimize for loading into a PostGreSQL database by writing

CSV files:
    combine multiple files into a single CSV file for loading
    entries do not exceed fixed field length
    order of fields is the same as the schema

Writes an optimized PSQL command for bulk loading files into the database.


truncate table sbm_covid19_hi_cdm_build.concept;
\copy sbm_covid19_hi_cdm_build.concept from ./CONCEPT.csv  WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';"
"""

import sys
import os
import argparse
import json
import sqlalchemy as sa
import csv


try:
    from utility_functions import generate_db_dict
except ImportError:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, os.path.pardir, "src")))
    from utility_functions import generate_db_dict


def main(target_directory, connection_string, target_schema):


    csv_files_to_load = generate_db_dict()

    tables_to_load = []
    tables_to_load_with_files = {}
    for file_name in csv_files_to_load:
        table_name = csv_files_to_load[file_name]
        if table_name not in tables_to_load:
            tables_to_load += [table_name]
            tables_to_load_with_files[table_name] = [file_name]
        else:
            tables_to_load_with_files[table_name] += [file_name]

    tables_to_load_with_schema = {t: target_schema + "." + t for t in tables_to_load}
    table_structure_json = os.path.join(target_directory, "table_structures.json")

    if not os.path.exists(table_structure_json):

        engine = sa.create_engine(connection_string)
        table_structures = {}
        with engine.connect() as connection:

            meta_data_obj = sa.MetaData(connection, schema=target_schema)
            meta_data_obj.reflect()

            for table_name in tables_to_load_with_schema:
                table_name_with_schema = tables_to_load_with_schema[table_name]

                table_obj = meta_data_obj.tables[table_name_with_schema]

                table_columns = []
                for column in table_obj.c:
                    if column.type.__class__ == sa.VARCHAR:
                        table_columns += [{"name": column.name, "type": str(column.type), "length": column.type.length}]
                    else:
                        table_columns += [{"name": column.name, "type": str(column.type)}]

                table_structures[table_name] = table_columns

            with open(table_structure_json,  "w") as fw:
                json.dump(table_structures, fw, sort_keys=True, indent=4, separators=(',', ': '))

    else:
        with open(table_structure_json, "r") as f:
            table_structures = json.load(f)

        tables_generated = []
        for table_name in tables_to_load_with_files:

            file_table_name = "load__" + table_name + ".csv"
            table_file_name = os.path.join(target_directory, file_table_name)

            tables_generated += [[table_name, table_file_name]]

            table_struct = table_structures[table_name]

            header = [t["name"] for t in table_struct]
            field_limits = {t["name"]: t["length"] for t in table_struct if "length" in t}
            column_positions = {table_struct[i]["name"]: i for i in range(len(header))}

            print(f"Generating: {table_file_name}")

            with open(table_file_name, "w", newline="", encoding="utf8", error="replace") as fw:
                csv_writer = csv.writer(fw)
                csv_writer.writerow(header)

                for csv_file_name in tables_to_load_with_files[table_name]:
                    full_csv_file_name = os.path.join(target_directory, csv_file_name)
                    print(f"\tProcessing: {full_csv_file_name}")

                    i = 0
                    with open(full_csv_file_name, "r", encoding="utf8", errors="replace") as f:
                        dict_reader = csv.DictReader(f)
                        for row_dict in dict_reader:
                            new_row = [''] * len(header)
                            for column in header:
                                if column in row_dict:
                                    column_position = column_positions[column]
                                    if column in field_limits:

                                        item = row_dict[column]
                                        if len(item) > field_limits[column]:
                                            item = item[0:field_limits[column]]
                                        new_row[column_position] = item
                                    else:
                                        new_row[column_position] = row_dict[column]

                            csv_writer.writerow(new_row)
                            i += 1

                        print(f"\tWrote {i} rows")

        with open(os.path.join(target_directory, "load_psql_cdm_tables.sql"), "w") as fw:
            for table_pairs in tables_generated:
                table_name, file_name = table_pairs

                fw.write(f"truncate table {target_schema}.{table_name};\n")
                fw.write(f"\\COPY {target_schema}.{table_name} from '{file_name}' with DELIMITER ',' NULL ''  CSV HEADER QUOTE '\"';\n\n")


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser(description="Generates cleaned CSV files for bulk uploading and a PSQL loading script")
    arg_parse_obj.add_argument("-c", "--config-file-name", dest="config_file_name", help="JSON config file",
                               default="../hf_config.json")

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