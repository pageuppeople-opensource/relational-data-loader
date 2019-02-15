import io
import os
import logging
from modules.ColumnTypeResolver import ColumnTypeResolver

from sqlalchemy import MetaData, DateTime, Boolean, BigInteger
from sqlalchemy.schema import Column, Table
from sqlalchemy.sql import func


class DestinationTableManager(object):
    TIMESTAMP_COLUMN_NAME = "data_pipeline_timestamp"
    IS_DELETED_COLUMN_NAME = "data_pipeline_is_deleted"
    CHANGE_VERSION_COLUMN_NAME = "data_pipeline_change_version"

    def __init__(self, target_db, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.target_db = target_db
        self.column_type_resolver = ColumnTypeResolver()

    def create_schema(self, schema_name):
        self.target_db.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def table_exists(self, schema_name, table_name):
        return self.target_db.dialect.has_table(self.target_db, table_name, schema_name)

    def drop_table(self, schema_name, table_name):
        metadata = MetaData()
        self.logger.debug(f"Dropping table {schema_name}.{table_name}")

        table = Table(table_name, metadata, schema=schema_name)
        table.drop(self.target_db, checkfirst=True)

        self.logger.debug(f"Dropped table {schema_name}.{table_name}")

    def create_table(self, schema_name, table_name, columns_configuration, drop_first):
        metadata = MetaData()

        table = Table(table_name, metadata, schema=schema_name)

        for column_configuration in columns_configuration:
            table.append_column(self.create_column(column_configuration['destination']))

        table.append_column(
            Column(self.TIMESTAMP_COLUMN_NAME, DateTime(timezone=True), server_default=func.now()))

        table.append_column(
            Column(self.IS_DELETED_COLUMN_NAME, Boolean, server_default='f', default=False))

        table.append_column(
            Column(self.CHANGE_VERSION_COLUMN_NAME, BigInteger))

        if drop_first:
            self.logger.debug(f"Dropping table {schema_name}.{table_name}")
            table.drop(self.target_db, checkfirst=True)
            self.logger.debug(f"Dropped table {schema_name}.{table_name}")

        self.logger.debug(f"Creating table {schema_name}.{table_name}")
        table.create(self.target_db, checkfirst=False)
        self.logger.debug(f"Created table {schema_name}.{table_name}")

        return

    def create_column(self, configuration):
        return Column(configuration['name'], self.column_type_resolver.resolve_postgres_type(configuration),
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

        old_load_table_name = f"{target_table_name}__old"

        # Step 1
        sql = f"DROP TABLE IF EXISTS {schema_name}.{old_load_table_name} CASCADE;  "
        self.logger.debug(f"Table Rename, executing '{sql}'")
        self.target_db.execute(sql)

        # Step 2
        sql_builder = io.StringIO()
        sql_builder.write("BEGIN TRANSACTION; ")

        # Step 3
        sql_builder.write(f"ALTER TABLE IF EXISTS {schema_name}.{target_table_name} RENAME TO {old_load_table_name}; ")

        # Step 4
        sql_builder.write(f"ALTER TABLE {schema_name}.{source_table_name} RENAME TO {target_table_name}; ")

        sql_builder.write("COMMIT TRANSACTION; ")
        self.logger.debug(f"Table Rename, executing '{sql_builder.getvalue()}'")
        self.target_db.execute(sql_builder.getvalue())

        sql_builder.close()

        sql = f"DROP TABLE IF EXISTS {schema_name}.{old_load_table_name} CASCADE "
        self.logger.debug(f"Table Rename, executing '{sql}'")
        self.target_db.execute(sql)

    def upsert_table(self, schema_name, source_table_name, target_table_name, columns_configuration):
        column_array = list(map(lambda column: column['destination']['name'], columns_configuration))
        column_list = ','.join(map(str, column_array))
        column_list = column_list + f",{self.TIMESTAMP_COLUMN_NAME}"
        column_list = column_list + f",{self.IS_DELETED_COLUMN_NAME}"
        column_list = column_list + f",{self.CHANGE_VERSION_COLUMN_NAME}"

        primary_key_column_array = [column_configuration['destination']['name'] for column_configuration in
                                    columns_configuration if 'primary_key' in column_configuration['destination'] and
                                    column_configuration['destination']['primary_key']]

        primary_key_column_list = ','.join(map(str, primary_key_column_array))

        sql_builder = io.StringIO()
        sql_builder.write(f"INSERT INTO {schema_name}.{target_table_name} ({column_list})")
        sql_builder.write(os.linesep)
        sql_builder.write(f" SELECT {column_list} FROM {schema_name}.{source_table_name}")
        sql_builder.write(os.linesep)
        sql_builder.write(f" ON CONFLICT({primary_key_column_list}) DO UPDATE SET ")

        for column_configuration in columns_configuration:
            sql_builder.write("{0} = EXCLUDED.{0},".format(column_configuration['destination']['name']))
            sql_builder.write(os.linesep)

        sql_builder.write("{0} = EXCLUDED.{0},".format(self.TIMESTAMP_COLUMN_NAME))
        sql_builder.write(os.linesep)
        sql_builder.write("{0} = EXCLUDED.{0},".format(self.IS_DELETED_COLUMN_NAME))
        sql_builder.write(os.linesep)
        sql_builder.write("{0} = EXCLUDED.{0}".format(self.CHANGE_VERSION_COLUMN_NAME))
        sql_builder.write(os.linesep)

        self.logger.debug(f"UPSERT executing '{sql_builder.getvalue()}'")
        self.target_db.execute(sql_builder.getvalue())
        self.logger.debug("UPSERT completed")

        sql_builder.close()
