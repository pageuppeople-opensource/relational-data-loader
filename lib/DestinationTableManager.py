import logging
from sqlalchemy import MetaData, DateTime
from sqlalchemy.schema import Column, Table
import importlib
from sqlalchemy.sql import func
import io
import os


class DestinationTableManager(object):
    TIMESTAMP_COLUMN_NAME = "data_pipeline_timestamp"

    def __init__(self, target_engine, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.target_engine = target_engine

    def create_schema(self, schema_name):
        self.target_engine.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(schema_name))

    def table_exists(self, schema_name, table_name):
        return self.target_engine.dialect.has_table(self.target_engine, table_name, schema_name)


    def drop_table(self, schema_name, table_name):
        metadata = MetaData()
        self.logger.debug(
            "Dropping table {0}.{1}".format(schema_name, table_name))

        table = Table(table_name, metadata, schema=schema_name)
        table.drop(self.target_engine, checkfirst=True)

        self.logger.debug(
            "Dropped table {0}.{1}".format(schema_name, table_name))




    def create_table(self, schema_name, table_name, columns_configuration, drop_first):
        metadata = MetaData()

        table = Table(table_name, metadata, schema=schema_name)

        for column_configuration in columns_configuration:
            table.append_column(self.create_column(column_configuration['destination']))

        table.append_column(
            Column(self.TIMESTAMP_COLUMN_NAME, DateTime(timezone=True), server_default=func.now()))

        if drop_first:
            self.logger.debug(
                "Dropping table {0}.{1}".format(schema_name, table_name))
            table.drop(self.target_engine, checkfirst=True)
            self.logger.debug(
                "Dropped table {0}.{1}".format(schema_name, table_name))

        self.logger.debug("Creating table {0}.{1}".format(schema_name, table_name))
        table.create(self.target_engine, checkfirst=False)
        self.logger.debug("Created table {0}.{1}".format(schema_name, table_name))
        return

    def create_column_type(self, type_name):
        parts = type_name.split(".")
        module = importlib.import_module(parts[0])
        class_ = getattr(module, parts[1])
        instance = class_()
        return instance

    def create_column(self, configuration):
        return Column(configuration['name'], self.create_column_type(configuration['type']),
                      primary_key=configuration.get("primary_key", False),
                      nullable=configuration['nullable'])

    def rename_table(self, schema_name, source_table_name, target_table_name):

        # Steps to efficiently rename a table.
        # 1. Drop target_old if exists.
        # 2. Begin transaction
        # 3. Rename target to target_old if it exists.
        # 4. Rename source to target
        # 5. commit
        # 6. Drop target_old if it exists.

        old_load_table_name = "{0}__old".format(target_table_name)

        # Step 1
        sql = "DROP TABLE IF EXISTS {0}.{1};  ".format(schema_name, old_load_table_name)
        self.logger.debug("Table Rename, executing {0} ".format(sql))
        self.target_engine.execute(sql)

        # Step 2
        sql_builder = io.StringIO()
        sql_builder.write("BEGIN TRANSACTION; ")

        # Step 3
        sql_builder.write(
            "ALTER TABLE IF EXISTS {0}.{1} RENAME TO {2}; ".format(schema_name, target_table_name, old_load_table_name))

        # Step 4
        sql_builder.write(
            "ALTER TABLE {0}.{1} RENAME TO {2}; ".format(schema_name, source_table_name, target_table_name))

        sql_builder.write("COMMIT TRANSACTION; ")
        self.logger.debug("Table Rename, executing {0}".format(sql_builder.getvalue()))
        self.target_engine.execute(sql_builder.getvalue())

        sql_builder.close()

        sql = "DROP TABLE IF EXISTS {0}.{1} ".format(schema_name, old_load_table_name)
        self.logger.debug("Table Rename, executing {0}".format(sql))
        self.target_engine.execute(sql)

    def upsert_table(self, schema_name, source_table_name, target_table_name, columns_configuration):
        column_array = list(map(lambda column: column['destination']['name'], columns_configuration))
        column_list = ','.join(map(str, column_array))
        column_list = column_list + ",{0}".format(self.TIMESTAMP_COLUMN_NAME)

        primary_key_column_array = [column_configuration['destination']['name'] for column_configuration in columns_configuration if 'primary_key' in column_configuration['destination'] and column_configuration['destination']['primary_key']]

        primary_key_column_list = ','.join(map(str, primary_key_column_array))

        sql_builder = io.StringIO()
        sql_builder.write("INSERT INTO {0}.{1} ({2})".format(schema_name, target_table_name, column_list))
        sql_builder.write(os.linesep)
        sql_builder.write(" SELECT {0} FROM {1}.{2}".format(column_list, schema_name, source_table_name))
        sql_builder.write(os.linesep)
        sql_builder.write(" ON CONFLICT({0}) DO UPDATE SET ".format(primary_key_column_list))

        for column_configuratiomn in columns_configuration:
            sql_builder.write("{0} = EXCLUDED.{0},".format(column_configuratiomn['destination']['name']))
            sql_builder.write(os.linesep)

        sql_builder.write("{0} = EXCLUDED.{0}".format(self.TIMESTAMP_COLUMN_NAME))

        self.logger.debug("Upsert executing {0}".format(sql_builder.getvalue()))
        self.target_engine.execute(sql_builder.getvalue())
        self.logger.debug("Upsert completed {0}")

        sql_builder.close()
