from flask import Flask
import sqlalchemy as sa
import json

app = Flask(__name__)


def connect_to_database(connection_string):
    engine = sa.create_engine(connection_string)
    connection = engine.connect()
    return connection


def find_concept_id_by_code(connection, vocabulary, code):

    meta_data = sa.MetaData(connection)
    meta_data.reflect()

    concept_obj = meta_data.tables["concept"]

    sel_obj = concept_obj.select().where(
        sa.and_(concept_obj.columns["vocabulary_id"] == vocabulary,
                concept_obj.columns["concept_code"] == code))

    cursor = connection.execute(sel_obj)
    results = list(cursor)

    concept_relation_obj = meta_data.tables["concept_relationship"]

    if len(results):
        result = results[0]
        return {"concept_id": result["concept_id"],
                "concept_name": result["concept_name"],
                "standard_concept": result["standard_concept"],
                "vocabulary_id": result["vocabulary_id"],
                "domain_id": result["domain_id"],
                "concept_code": result["concept_code"],
                "concept_class_id": result["concept_class_id"]
                }
    else:
        return []



@app.route("/code/<vocabulary>/<code>")
def find_by_vocabulary_code(vocabulary, code):

    connection = connect_to_database("sqlite:///ohdsi_concept.db3")
    result = find_concept_id_by_code(connection, vocabulary, code)

    return json.dumps(result)