import unittest
import json
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from modules.data_sources.MsSqlDataSource import MsSqlDataSource
import logging

TEST_DB = "RDLUnitTestSource"
MSSQL_STRING_FORMAT = "mssql+pyodbc://{username}:{password}@{server_string}/{db}?driver=SQL+Server+Native+Client+11.0"

SQL_PATH = "./modules/tests/setup/"
SQL_ORDERED_FILES = [
    {
        "db": "master",
        "file_name": "create_database.sql"
    },
    {
        "db": TEST_DB,
        "file_name": "create_simple_table.sql"
    }]

CONFIG_PATH = "./modules/tests/config/"
CONFIG_FILES = ["SimpleTableTest.json"]


class TestMsSqlDataSource(unittest.TestCase):
    MSSQL_DATA_SOURCE = None
    table_configurations = []

    @classmethod
    def setUpClass(cls):

        with open(CONFIG_PATH + "connection.json") as f:
            gen_connection_string = MSSQL_STRING_FORMAT.format(**json.loads(f.read()), db="{db}")
        cls.table_configurations = []

        for file_name in CONFIG_FILES:
            with open(CONFIG_PATH + file_name, "r") as f:
                json_data = json.loads(f.read())
                cls.table_configurations.append(json_data)

        for file_obj in SQL_ORDERED_FILES:
            with open(SQL_PATH + file_obj["file_name"], "r") as f:
                set_up_db_string = f.read()
                temp_eng = create_engine(gen_connection_string.format(db=file_obj["db"]),
                                         connect_args={'autocommit': True})
                temp_eng.execute(set_up_db_string.format(db=TEST_DB))

        TestMsSqlDataSource.MSSQL_DATA_SOURCE = MsSqlDataSource(gen_connection_string.format(db=TEST_DB))

    @classmethod
    def tearDownClass(cls):
        TEAR_DOWN_STRING = """
            USE [master];
            IF EXISTS (SELECT * FROM sys.databases WHERE Name = '{db}')
                DROP DATABASE [{db}];
        """.format(db=TEST_DB)

        TestMsSqlDataSource.MSSQL_DATA_SOURCE.database_engine.execute(text(TEAR_DOWN_STRING))
        TestMsSqlDataSource.MSSQL_DATA_SOURCE = None

    def test_init_change_tracking(self):

        last_sync_version = 'NULL'
        for table in TestMsSqlDataSource.table_configurations:
            print("TESTING ON TABLE: " + table["source_table"]["name"])
            print("FIRST TEST: INITIALISE TABLE")
            results = TestMsSqlDataSource.MSSQL_DATA_SOURCE.init_change_tracking(
                table["source_table"], last_sync_version)
            self.assertEqual(results.force_full_load, True)
            last_sync_version = results.next_sync_version

            print("SECOND TEST: NO CHANGES")
            results = TestMsSqlDataSource.MSSQL_DATA_SOURCE.init_change_tracking(
                table["source_table"], last_sync_version)
            self.assertEqual(results.force_full_load, False)
            last_sync_version = results.next_sync_version

            print("OPERATION TESTS")
            for operation_string in table["operation_strings"]:
                TestMsSqlDataSource.MSSQL_DATA_SOURCE.database_engine.execute(
                    text(operation_string).execution_options(autocommit=True))

                results = TestMsSqlDataSource.MSSQL_DATA_SOURCE.init_change_tracking(
                    table["source_table"], last_sync_version)
                self.assertEqual(results.force_full_load, False, msg="Failed on: " + operation_string)
                last_sync_version = results.next_sync_version

            print("EXTRA TEST: NO CHANGES")
            results = TestMsSqlDataSource.MSSQL_DATA_SOURCE.init_change_tracking(
                table["source_table"], last_sync_version)
            self.assertEqual(results.force_full_load, False)

            print("EXTRA TEST: LOST TRACK")
            results = TestMsSqlDataSource.MSSQL_DATA_SOURCE.init_change_tracking(table["source_table"], -1)
            self.assertEqual(results.force_full_load, True)

    def test_can_handle_connection_string(self):
        self.assertFalse(MsSqlDataSource.can_handle_connection_string("postgresql+psycopg2://postgres"))
        self.assertTrue(MsSqlDataSource.can_handle_connection_string(MSSQL_STRING_FORMAT))

    def test_prefix_column(self):
        col = "foo"
        col_fail = "BAR"
        p_key_cols = ["foo", "bar"]
        self.assertEqual("chg." + col, MsSqlDataSource.prefix_column(col, False, p_key_cols))
        self.assertEqual("t." + col_fail, MsSqlDataSource.prefix_column(col_fail, False, p_key_cols))
        self.assertEqual("t." + col, MsSqlDataSource.prefix_column(col, True, p_key_cols))
        self.assertEqual("t." + col_fail, MsSqlDataSource.prefix_column(col_fail, True, p_key_cols))
        self.assertRaises(TypeError, MsSqlDataSource.prefix_column, (col, True, "some string"))


if __name__ == '__main__':
    unittest.main()
