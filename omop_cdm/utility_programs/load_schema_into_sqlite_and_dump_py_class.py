"""
    Just a small script to load the OMOP CDM into a SQLite database
    for manipulation.
"""

import sqlalchemy as sa
import os
import path
import sys

from generate_io_classes_file_from_source import *


def load_sql(schema_file_name="../schema/omop_cdm.sql", sa_connection_string="sqlite:///cdm_v5.db3"):

    engine = sa.create_engine(sa_connection_string)
    connection = engine.connect()

    with open(schema_file_name, "r") as f:
        schema_sql = f.read()

    sql_statements = schema_sql.split(";")

    for sql_statement in sql_statements:
        connection.execute(sql_statement)


if __name__ == "__main__":

    if os.path.exists("cdm_v5.db3"):
        os.remove("cdm_v5.db3")

    load_sql()

    with open("./omop_cdm_classes.py", "w") as f:
        f.write("from mapping_classes import OutputClass\n\n")
        f.write(generate_sql_from_connection_string("sqlite:///cdm_v5.db3"))

