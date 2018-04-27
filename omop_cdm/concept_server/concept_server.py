from flask import Flask
import sqlalchemy as sa
import json

app = Flask(__name__)


def connect_to_database(connection_string):
    engine = sa.create_engine(connection_string)
    connection = engine.connect()
    return connection


def convert_to_result_dict(result):
    result_dict = {"concept_id": result["concept_id"],
                   "concept_name": result["concept_name"],
                   "standard_concept": result["standard_concept"],
                   "vocabulary_id": result["vocabulary_id"],
                   "domain_id": result["domain_id"],
                   "concept_code": result["concept_code"],
                   "concept_class_id": result["concept_class_id"]}
    return result_dict


def find_concept_id_by_code(connection, vocabulary, code):

    meta_data = sa.MetaData(connection)
    meta_data.reflect()

    concept_obj = meta_data.tables["concept"]

    concept_sel_obj = concept_obj.select().where(
        sa.and_(concept_obj.columns["vocabulary_id"] == vocabulary,
                concept_obj.columns["concept_code"] == code))

    cursor = connection.execute(concept_sel_obj)
    results = list(cursor)

    concept_relation_obj = meta_data.tables["concept_relationship"]

    if len(results):
        result = results[0]

        result_dict = convert_to_result_dict(result)

        result_dict["mapped_standard_concept"] = {}
        if result_dict["standard_concept"] == "S":
            result_dict["is_standard_concept"] = True
        else:
            result_dict["is_standard_concept"] = False

            concept_rel_select_obj = concept_relation_obj.select().where(
                sa.and_(concept_relation_obj.columns["concept_id_1"] == result_dict["concept_id"],
                        concept_relation_obj.columns["relationship_id"] == "Maps to"
                        )
            )

            cursor = connection.execute(concept_rel_select_obj)
            result_relationships = list(cursor)

            if len(result_relationships):
                result_relationship = result_relationships[0]
                mapped_concept_id = result_relationship.concept_id_2

                concept_mapped_sel_obj = concept_obj.select().where(
                    concept_obj.columns["concept_id"] == mapped_concept_id)
                cursor = connection.execute(concept_mapped_sel_obj)
                result_concept_mapped = list(cursor)

                if len(result_concept_mapped):
                    result_mapped_dict = convert_to_result_dict(result_concept_mapped[0])
                    result_dict["mapped_standard_concept"] = result_mapped_dict

        return result_dict

    else:
        return {}


@app.route("/code/<vocabulary>/<code>")
def find_by_vocabulary_code(vocabulary, code):

    connection = connect_to_database("sqlite:///ohdsi_concept.db3")
    result = find_concept_id_by_code(connection, vocabulary, code)

    return json.dumps(result)