import argparse
import json
import sqlparse
import sqlalchemy as sa


def main(schema_file_name, connection_string, db_schema,
         schema_customization_file_name=None,
         index_file_name=None,
         constraints_file_name=None,
         vocab_schema=None,
         post_data_manipulation_schema=None
         ):

    with open(schema_file_name, "r") as f:
        schema_sql = f.read()

    engine = sa.create_engine(connection_string)
    connection = engine.connect()

    if db_schema is not None:
        create_schema_if_does_not_exists(connection, db_schema)

    # Build schema
    execute_sql(connection, schema_sql, db_schema)

    if schema_customization_file_name is not None:

        with open(schema_customization_file_name, "r") as f:
            schema_customization_sql = f.read()
            execute_sql(connection, schema_customization_sql, db_schema)

    # Custom schema alterations
    if schema_customization_file_name:
        pass

    # Copy schema from another source table
    if vocab_schema:
        pass

    # Build indices
    if index_file_name:
        pass

    # Add constraints
    if constraints_file_name:
        pass

    # Add post data
    if post_data_manipulation_schema:
        pass

def execute_sql(connection, code_to_execute, schema=None):

    sql_statements = sqlparse.split(code_to_execute)

    if schema is not None:
        pre_statement = "set search_path=%s;" % schema
    else:
        pre_statement = ""

    i = 0
    for sql_statement in sql_statements:
        print(sql_statement)
        sql_to_execute = pre_statement + sql_statement
        connection.execute(sql_to_execute)
        i += 1

    print("Executed %s statements" % i)


def create_schema_if_does_not_exists(connection, schema):

    cursor = connection.execute("select exists (select * from pg_catalog.pg_namespace where nspname = '%s')" % schema)
    schema_exists = list(cursor)[0][0]

    if schema_exists:
        pass
    else:
        print("Creating schema '%s'" % schema)
        connection.execute("create schema %s" % schema)


if __name__ == "__main__":
    arg_parse_obj = argparse.ArgumentParser()
    arg_parse_obj.add_argument("-c", "--configuration", dest="configuration_json")

    arg_obj = arg_parse_obj.parse_args()
    config_json_file_name = arg_obj.configuration_json
    with open(config_json_file_name, "r") as f:
        config_dict = json.load(f)