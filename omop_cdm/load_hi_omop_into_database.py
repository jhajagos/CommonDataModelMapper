import os
import csv
import sqlalchemy as sa
import datetime
import pprint
import time
import json


def load_csv_files_into_db(connection_string, data_dict, schema_ddl=None, indices_ddl=None, schema=None, delimiter=",",
                           lower_case_keys=True, i_print_update=1000):
    db_engine = sa.create_engine(connection_string)
    db_connection = db_engine.connect()

    if schema_ddl is not None:
        split_sql = schema_ddl.split(";")
        for sql_statement in split_sql:
            db_connection.execute(sql_statement)

    meta_data = sa.MetaData(db_connection, reflect=True, schema=schema)
    for data_file in data_dict:

        table_name = data_dict[data_file]
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


def main(output_directory=None, vocabulary_directory=None, load_vocabularies=False, load_data=True):
    with open("./omop_cdm.sql") as f:
        omop_cdm_sql = f.read()

    with open("./omop_cdm_indexes.sql") as f:
        omop_cdm_idx_sql = f.read()

    output_sqlite3 = os.path.join(output_directory, "omop_db_load.db3")

    if os.path.exists(output_sqlite3):
        os.remove(output_sqlite3)

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

    with open("./hi_config.json") as f:
        config = json.load(f)

    main(output_directory=config["csv_output_directory"], vocabulary_directory=config["json_map_directory"], load_vocabularies=True)