import logging
from sqlalchemy import MetaData, DateTime
from sqlalchemy.schema import Column, Table
import importlib
from sqlalchemy.sql import func


class DestinationTableManager(object):
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def create_table(self, table_configuration, columns_configuration, target_engine, drop_first):
        metadata = MetaData()

        table = Table(table_configuration['name'], metadata, schema=table_configuration['schema'])

        for column_configuration in columns_configuration:
            table.append_column(self.create_column(column_configuration['destination']))

        table.append_column(
            Column("data_pipeline_timestamp", DateTime(timezone=True), server_default=func.now()))

        if drop_first:
            self.logger.info(
                "Dropping table {0}.{1}".format(table_configuration['name'], table_configuration['schema']))
            table.drop(target_engine, checkfirst=True)
            self.logger.debug(
                "Dropped table {0}.{1}".format(table_configuration['name'], table_configuration['schema']))

        self.logger.info("Creating table {0}.{1}".format(table_configuration['name'], table_configuration['schema']))
        table.create(target_engine, checkfirst=False)
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
                      nullable=configuration['nullable']);

    def rename_table(self, source_table_configuration, target_table_configuration):
        print('TODO - create a rename-table method. Eg: ALTER TABLE table_name RENAME TO new_table_name;')
        return;

    def upsert_data_from_stage_to_load_tables(self, source_table_configuration, target_table_configuration):
        print('TODO - create a method to upsert the data;')
        return;
