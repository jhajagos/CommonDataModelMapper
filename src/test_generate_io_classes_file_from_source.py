import unittest
import os

from generate_io_classes_file_from_source import *
from mapping_classes import *
import sqlalchemy as sa


class TestClassGeneration(unittest.TestCase):

    def setUp(self):

        if os.path.exists("./test/test.db3"):
            os.remove("./test/test.db3")

        with open("./test/create_test_tables.sql", "r") as f:
            create_table_sql = f.read()

        engine = sa.create_engine("sqlite:///./test/test.db3")
        connection = engine.connect()
        for statement in create_table_sql.split(";"):
            print(statement)
            connection.execute(statement)

    def test_class_generate(self):
        simple_class = input_output_class_generate_string("Object1", "OutputClass", ["id", "test_name", "code_name"])

        exec(simple_class)
        self.assertTrue(len(simple_class))

    def test_generate_from_db(self):
        generated_classes_py = generate_sql_from_connection_string("sqlite:///./test/test.db3")

        print(generated_classes_py)
        self.assertEquals(True, False)


if __name__ == '__main__':
    unittest.main()
