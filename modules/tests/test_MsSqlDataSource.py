import unittest
import json
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from modules.data_sources.MsSqlDataSource import MsSqlDataSource
import logging

TEST_DB = "RelationalDataLoaderIntegrationTestSource"
MSSQL_STRING_FORMAT = "mssql+pyodbc://{username}:{password}@{server_string}/{db}?driver=SQL+Server+Native+Client+11.0"


class TestMsSqlDataSource(unittest.TestCase):

    def setUp(self):
        self.sql_path = "./modules/tests/setup/"
        self.sql_files_order = [
            {
                "db": "master",
                "file_name": "create_database.sql"
            },
            {
                "db": TEST_DB,
                "file_name": "create_simple_table.sql"
            }]

        self.config_path = "./modules/tests/config/"
        self.config_files = ["SimpleTableTest.json"]

        with open(self.config_path + "connection.json") as f:
            self.generic_connection_string = MSSQL_STRING_FORMAT.format(**json.loads(f.read()), db="{db}")

        self.mssql_data_source = MsSqlDataSource(self.generic_connection_string.format(db=TEST_DB))

        self.table_configurations = []

        for file_name in self.config_files:
            with open(self.config_path+file_name, "r") as f:
                json_data = json.loads(f.read())
                self.table_configurations.append(json_data)

        for file_obj in self.sql_files_order:
            with open(self.sql_path+file_obj["file_name"], "r") as f:
                set_up_db_string = f.read()
                temp_eng = create_engine(self.generic_connection_string.format(db=file_obj["db"]),
                                         connect_args={'autocommit': True})
                temp_eng.execute(set_up_db_string.format(db=TEST_DB))

    def tearDown(self):
        TEAR_DOWN_STRING = """
            USE [master];
            IF EXISTS (SELECT * FROM sys.databases WHERE Name = '{db}')
                DROP DATABASE [{db}];
        """.format(db=TEST_DB)

        self.mssql_data_source.database_engine.execute(text(TEAR_DOWN_STRING))

    def test_init_change_tracking(self):

        last_sync_version = 0
        for table in self.table_configurations:
            print("TESTING ON TABLE: " + table["source_table"]["name"])
            print("FIRST TEST: INITIALISE TABLE")
            results = self.mssql_data_source.init_change_tracking(table["source_table"], 'NULL')
            self.assertEqual(results.force_full_load, True)
            last_sync_version = results.next_sync_version

            print("SECOND TEST: NO CHANGES")
            results = self.mssql_data_source.init_change_tracking(table["source_table"], last_sync_version)
            self.assertEqual(results.force_full_load, False)
            last_sync_version = results.next_sync_version

            print("OPERATION TESTS")
            for operation_string in table["operation_strings"]:
                self.mssql_data_source.database_engine.execute(
                    text(operation_string).execution_options(autocommit=True))

                results = self.mssql_data_source.init_change_tracking(table["source_table"], last_sync_version)
                self.assertEqual(results.force_full_load, False, msg="Failed on: " + operation_string)
                last_sync_version = results.next_sync_version

            print("EXTRA TEST: NO CHANGES")
            results = self.mssql_data_source.init_change_tracking(table["source_table"], last_sync_version)
            self.assertEqual(results.force_full_load, False)

            print("EXTRA TEST: LOST TRACK")
            results = self.mssql_data_source.init_change_tracking(table["source_table"], -1)
            self.assertEqual(results.force_full_load, True)


if __name__ == '__main__':
    unittest.main()
