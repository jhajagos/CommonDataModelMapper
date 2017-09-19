import argparse
import sqlparse
import sqlalchemy as sa


def copy_into_table(connection, table_name, source_schema, destination_schema):

    trans = connection.begin()

    try:
        sql_to_execute = "insert into %s.%s select * from %s.%s" % (destination_schema, table_name, source_schema, table_name)
        print(sql_to_execute)
        connection.execute(sql_to_execute)
    except:
        trans.rollback()
        raise

    trans.commit()


def main(ddl_file_name, connection_string, db_schema,
         schema_customization_file_name=None,
         index_file_name=None,
         constraints_file_name=None,
         vocab_schema=None,
         post_data_manipulation_file_name=None,
         drop_tables=None
         ):

    engine = sa.create_engine(connection_string)
    connection = engine.connect()

    if db_schema is not None:
        create_schema_if_does_not_exists(connection, db_schema)

    if drop_tables and db_schema is not None:
        metadata = sa.MetaData(connection, schema=db_schema, reflect=True)

        metadata.drop_all()


    # Build schema
    if ddl_file_name:
        execute_sql_file(connection, ddl_file_name, db_schema)

    if schema_customization_file_name is not None:
        execute_sql_file(connection, schema_customization_file_name, db_schema)

    # Custom schema alterations
    if schema_customization_file_name:
        execute_sql_file(connection, schema_customization_file_name, db_schema)

    # Copy schema from another source table
    if vocab_schema:
        # Copy into schema
        vocabularies = ["CONCEPT",
                        "CONCEPT_ANCESTOR",
                        "CONCEPT_CLASS",
                        "CONCEPT_RELATIONSHIP",
                        "CONCEPT_SYNONYM",
                        "DOMAIN",
                        "DRUG_STRENGTH",
                        "RELATIONSHIP",
                        "VOCABULARY"]

        for vocabulary in vocabularies:
            copy_into_table(connection, vocabulary, vocab_schema, db_schema)

    # Build indices
    if index_file_name:
        execute_sql_file(connection, index_file_name, db_schema)

    # Add constraints
    if constraints_file_name:
        execute_sql_file(connection, constraints_file_name, db_schema)

    # Perform post data manipulation
    if post_data_manipulation_file_name:
        execute_sql_file(connection, post_data_manipulation_file_name, db_schema)


def execute_sql_file(connection, file_name, schema):
    with open(file_name, "r") as f:
        sql_file_source = f.read()
    execute_sql(connection, sql_file_source, schema)


def execute_sql(connection, code_to_execute, schema=None):

    sql_statements = sqlparse.split(code_to_execute)

    if schema is not None:
        pre_statement = "set search_path=%s;" % schema
    else:
        pre_statement = ""

    trans = connection.begin()
    try:
        i = 0
        for sql_statement in sql_statements:
            print(sql_statement)
            sql_to_execute = pre_statement + sql_statement
            connection.execute(sql_to_execute)
            i += 1
    except:
        trans.rollback()
        raise

    trans.commit()
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
    arg_parse_obj.add_argument("--connection-uri", dest="connection_uri", default=None)
    arg_parse_obj.add_argument("--schema", dest="schema", default=None)
    arg_parse_obj.add_argument("--schema-customization-file", dest="schema_customization_file_name", default=None)
    arg_parse_obj.add_argument("--drop-tables", dest="drop_tables", default=False, action="store_true")
    arg_parse_obj.add_argument("--constraints-file", dest="constraints_file_name", default=None)
    arg_parse_obj.add_argument("--ddl-file", dest="ddl_file_name", default=None)
    arg_parse_obj.add_argument("--index-file", dest="index_file_name", default=None)
    arg_parse_obj.add_argument("--post-data-manipulation-file", dest="post_data_manipulation_file_name", default=None)
    arg_parse_obj.add_argument("--vocabulary-schema", dest="vocab_schema")

    arg_obj = arg_parse_obj.parse_args()

    connection_uri = arg_obj.connection_uri
    schema = arg_obj.schema
    schema_customization_file_name = arg_obj.schema_customization_file_name
    constraints_file_name = arg_obj.constraints_file_name
    index_file_name = arg_obj.index_file_name
    ddl_file_name = arg_obj.ddl_file_name
    post_data_manipulation_file_name = arg_obj.post_data_manipulation_file_name
    vocabulary_schema = arg_obj.vocab_schema
    drop_tables = arg_obj.drop_tables

    main(ddl_file_name, connection_uri, schema, schema_customization_file_name, index_file_name,
         constraints_file_name, vocabulary_schema, post_data_manipulation_file_name, drop_tables)