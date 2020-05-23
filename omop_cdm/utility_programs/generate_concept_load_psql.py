import os
import argparse


def main(schema, path_to_concept_files="./", psql_load_script="ohdsi_load_concepts.sql"):

    generated_sql = f"""
truncate table {schema}.concept;
\\copy {schema}.concept from {os.path.join(path_to_concept_files, "CONCEPT.csv")}  WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';

truncate table {schema}.concept_relationship;
\\copy {schema}.concept_relationship from {os.path.join(path_to_concept_files, "CONCEPT_RELATIONSHIP.csv")}  WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';
    
truncate table {schema}.concept_ancestor;
\\copy {schema}.concept_ancestor from {os.path.join(path_to_concept_files, "CONCEPT_ANCESTOR.csv")}  WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';
    
truncate table {schema}.concept_synonym;
\\copy {schema}.concept_synonym from {os.path.join(path_to_concept_files, "CONCEPT_SYNONYM.csv")}  WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';
    
truncate table {schema}.drug_strength;
\\copy {schema}.drug_strength from {os.path.join(path_to_concept_files, "DRUG_STRENGTH.csv")}  WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';
    
truncate table {schema}.vocabulary;
\\copy {schema}.vocabulary from {os.path.join(path_to_concept_files, "VOCABULARY.csv")}  WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';
    
truncate table {schema}.concept_class;
\\copy {schema}.concept_class from {os.path.join(path_to_concept_files, "CONCEPT_CLASS.csv")}  WITH DELIMITER E'\\t' CSV HEADER QUOTE E'\\b';
"""

    with open(psql_load_script, "w") as fw:
        fw.write(generated_sql)


if __name__ == "__main__":
    arg_obj = argparse.ArgumentParser(description="Build PSQL script to load OHDSI Concept tables")

    arg_obj.add_argument("-s", "--schema-name", dest="schema_name")
    arg_obj.add_argument("-c", "--concept-directory-path", dest="concept_directory_path", default="./")
    arg_obj.add_argument("-f", "--sql-file-name", dest="sql_file_name", default="ohdsi_load_concepts.sql")

    arg_parse_obj = arg_obj.parse_args()
    main(arg_parse_obj.schema_name, arg_parse_obj.concept_directory_path, arg_parse_obj.sql_file_name)