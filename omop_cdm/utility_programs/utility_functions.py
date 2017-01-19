import csv
import sqlalchemy as sa
import datetime
import pprint
import time
import os


def load_csv_files_into_db(connection_string, data_dict, schema_ddl=None, indices_ddl=None, schema=None, delimiter=",",
                           lower_case_keys=True, i_print_update=1000, truncate=False):
    db_engine = sa.create_engine(connection_string)
    db_connection = db_engine.connect()

    table_names = []
    if schema_ddl is not None:
        split_sql = schema_ddl.split(";")
        for sql_statement in split_sql:
            db_connection.execute(sql_statement)

    for key in data_dict:
        table_name = data_dict[key]
        if table_name not in table_names:
            if schema:
                table_name = schema + "." + table_name
            table_names += [table_name]

    if truncate:
        for table_name in table_names:
            truncate_sql = "truncate %s" % table_name
            db_connection.execute(truncate_sql)

    meta_data = sa.MetaData(db_connection, reflect=True, schema=schema)
    for data_file in data_dict:

        table_name = data_dict[data_file]
        if schema:
            table_name = schema + "." + table_name

        table_obj = meta_data.tables[table_name]

        print("Loading %s" % table_name)

        db_transaction = db_connection.begin()
        try:
            with open(data_file) as f:
                dict_reader = csv.DictReader(f, delimiter=delimiter)
                start_time = time.time()
                elapsed_time = start_time
                i = 0
                for dict_row in dict_reader:
                    cleaned_dict = {}
                    for key in dict_row:
                        if len(dict_row[key]):

                            if "date" in key or "DATE" in key:
                                if "-" in dict_row[key]:
                                    cleaned_dict[key] = datetime.datetime.strptime(dict_row[key], "%Y-%m-%d")
                                else:
                                    cleaned_dict[key] = datetime.datetime.strptime(dict_row[key], "%Y%m%d")
                            else:
                                cleaned_dict[key] = dict_row[key]

                    if lower_case_keys:
                        temp_cleaned_dict = {}
                        for key in cleaned_dict:
                            temp_cleaned_dict[key.lower()] = cleaned_dict[key]
                        cleaned_dict = temp_cleaned_dict

                    s = table_obj.insert(cleaned_dict)
                    try:
                        db_connection.execute(s)
                    except:
                        pprint.pprint(cleaned_dict)
                        raise

                    if i > 0 and i % i_print_update == 0:
                        current_time = time.time()
                        time_difference = current_time - elapsed_time
                        print("Loaded %s total rows at %s seconds per %s rows" % (i, time_difference, i_print_update))
                        elapsed_time = time.time()
                    i += 1

                db_transaction.commit()
                current_time = time.time()
                total_time_difference = current_time - start_time
                print("Loaded %s total row in %s seconds" % (i, total_time_difference))

        except:
            db_transaction.rollback()
            raise

    if indices_ddl is not None:
        split_sql = indices_ddl.split(";")
        for sql_statement in split_sql:
            db_connection.execute(sql_statement)

def generate_db_dict(output_directory):
    load_pairs = [("condition_occurrence", "condition_occurrence_dx_cdm.csv"),
                  ("person", "person_cdm.csv"),
                  ("visit_occurrence", "visit_occurrence_cdm.csv"),
                  ("procedure_occurrence", "procedure_cdm.csv"),
                  ("procedure_occurrence", "procedure_dx_cdm.csv"),
                  ("measurement", "measurement_encounter_cdm.csv"),
                  ("measurement", "measurement_dx_cdm.csv"),
                  ("measurement", "measurement_proc_cdm.csv"),
                  ("drug_exposure", "drug_exposure_cdm.csv"),
                  ("drug_exposure", "drug_exposure_proc_cdm.csv"),
                  ("death", "death_cdm.csv"),
                  ("observation", "observation_dx_cdm.csv"),
                  ("observation", "observation_measurement_encounter_cdm.csv"),
                  ("observation", "observation_proc_cdm.csv"),
                  ("observation_period", "observation_period_cdm.csv")
                 ]

    data_dict = {}
    for pair in load_pairs:
        data_dict[os.path.join(output_directory, pair[1])] = pair[0]

    return data_dict