import os
import sys
import sqlalchemy as sa
import json
import argparse

try:
    from utility_functions import load_csv_files_into_db, generate_vocabulary_load
except(ImportError):
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.split(__file__)[0], os.path.pardir, os.path.pardir, "src")))
    from utility_functions import load_csv_files_into_db, generate_vocabulary_load


def main(data_directory, vocabulary_directory):

    concept_db3 = os.path.join(data_directory, "ohdsi_concept.db3")

    concept_sql = """   
        CREATE TABLE concept (
          concept_id			    INTEGER			  NOT NULL ,
          concept_name			  VARCHAR(255)	NOT NULL ,
          domain_id				    VARCHAR(20)		NOT NULL ,
          vocabulary_id			  VARCHAR(20)		NOT NULL ,
          concept_class_id		VARCHAR(20)		NOT NULL ,
          standard_concept		VARCHAR(1)		NULL ,
          concept_code			  VARCHAR(50)		NOT NULL ,
          valid_start_date		DATE			    NOT NULL ,
          valid_end_date		  DATE			    NOT NULL ,
          invalid_reason		  VARCHAR(1)		NULL
        )"""

    concept_relationship_sql = """
    CREATE TABLE concept_relationship (
      concept_id_1			INTEGER			NOT NULL,
      concept_id_2			INTEGER			NOT NULL,
      relationship_id		VARCHAR(20)	NOT NULL,
      valid_start_date	DATE			  NOT NULL,
      valid_end_date		DATE			  NOT NULL,
      invalid_reason		VARCHAR(1)	NULL
      )
    """

    if os.path.exists(concept_db3):
        os.remove(concept_db3)

    connection_string = "sqlite:///" + data_directory + concept_db3
    engine = sa.create_engine(connection_string)

    connection = engine.connect()
    connection.execute(concept_sql)
    connection.execute(concept_relationship_sql)

    indexes = [
         "create unique index pk_index_1 on concept(concept_id)",
         "create index idx_vocab on concept(vocabulary_id)",
         "create index idx_concept_code on concept(concept_code)",
         "create index idx_concept_name on concept(concept_name)",
         "create index idx_concept_c1 on concept_relationship(concept_id_1)",
         "create index idx_concept_c2 on concept_relationship(concept_id_2)"]

    for index in indexes:
        connection.execute(index)

    connection.close()

    concepts = ["concept", "concept_relationship"]

    concept_tables = []
    for concept in concepts:
        concept_tables += [(concept.lower(), os.path.join(vocabulary_directory, concept + ".csv"))]

    concept_data_dict = {}
    for pair in concept_tables:
        concept_data_dict[pair[1]] = pair[0]

    load_csv_files_into_db(connection_string, concept_data_dict, schema_ddl=None, indices_ddl=None,
                           i_print_update=1000, truncate=False, schema=None, delimiter="\t")

    connection = engine.connect()


if __name__ == "__main__":

    arg_obj = argparse.ArgumentParser("Load concept tables into a SQLite database")
    arg_obj.add_argument("-c", "--config-json-file", dest="config_json", default="config.json")
    arg_parse_obj = arg_obj.parse_args()

    with open(arg_parse_obj.config_json) as f:
        config = json.load(f)

    main(config["data_directory"], config["json_map_directory"])