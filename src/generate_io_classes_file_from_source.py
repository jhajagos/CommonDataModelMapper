"""
    Outputs python source code which defines either the input or output
    class to be used in defining mappings across two databases.
"""

import sqlalchemy as sa


def input_output_class_generate_string(class_name, parent_class_name, fields, indent="    "):

    class_py_string = "class " + class_name + "(" + parent_class_name + "):\n"
    class_py_string += indent + "def fields(self):\n"

    fields_string = "["
    for field in fields:
        fields_string += '"' + field + '", '

    fields_string = fields_string[:-2]

    fields_string += "]"

    class_py_string += indent + indent + "return " + fields_string + "\n"

    return class_py_string

def transform_table_name_to_class_name(table_name, suffix="Object"):
    """"Transform a table name encounter_event into EncounterEventObject"""
    table_name_split = table_name.split("_")
    class_name  = ""
    for name_part in table_name_split:
        class_name += name_part[0].upper() + name_part.lower()[1:]
    class_name += suffix
    return class_name


def generate_sql_from_connection_string(connection_string, parent_class_name="OutputClass"):
    engine = sa.create_engine(connection_string)
    connection = engine.connect()
    meta_data = sa.MetaData(engine)
    meta_data.reflect()

    table_names = meta_data.tables.keys()

    class_libaray_py = ""
    for table_name in table_names:
        table_class_name = transform_table_name_to_class_name(table_name)
        field_names = meta_data.tables[table_name].columns.keys()
        class_libaray_py += input_output_class_generate_string(table_class_name, parent_class_name, field_names)
        class_libaray_py += "\n\n"

    print(class_libaray_py)
    raise
    return class_libaray_py

