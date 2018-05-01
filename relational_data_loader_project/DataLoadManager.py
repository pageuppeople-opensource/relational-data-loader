import logging
from relational_data_loader_project.BatchDataLoader import BatchDataLoader
from relational_data_loader_project.DestinationTableManager import DestinationTableManager
from relational_data_loader_project.DataLoadTracker import DataLoadTracker
from relational_data_loader_project.SourceTableManager import SourceTableManager
import os
import json


class DataLoadManager(object):
    def __init__(self, configuration_path, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.configuration_path = configuration_path

    def start_import(self, source_engine, target_engine, full_load):
        for file in os.listdir(self.configuration_path):
            self.start_single_import(source_engine, target_engine, file, full_load)

    def start_single_import(self, source_engine, target_engine, configuration_name, full_load):

        with open("{0}{1}".format(self.configuration_path, configuration_name)) as json_data:
            pipeline_configuration = json.load(json_data)

        data_load_tracker = DataLoadTracker(configuration_name, json_data, full_load)

        self.logger.debug("Execute Starting")
        destination_table_manager = DestinationTableManager()

        columns = self.remove_invalid_columns(pipeline_configuration['source_table'], pipeline_configuration['columns'],
                                              source_engine)

        if full_load:
            self.logger.info("Full-load is set. Recreating the staging table.")
            destination_table_manager.create_table(pipeline_configuration['stage_table'],
                                                   columns, target_engine, drop_first=True)

        # Import the data.
        batch_importer = BatchDataLoader(pipeline_configuration['source_table'], columns,
                                         pipeline_configuration['batch'])

        previous_unique_column_value = 0
        while previous_unique_column_value > -1:
            previous_unique_column_value = batch_importer.import_batch(source_engine, target_engine, pipeline_configuration['stage_table'], data_load_tracker.start_batch(), previous_unique_column_value)


        self.logger.info("ImportBatch Completed")

        #if full_load:
            #return
            # Rename the stage table to the load table.
            # log.information("Full-load is set. Renaming the stage table to the load table.")
            # rename_table(pipeline_configuration['stage_source_data'], pipeline_configuration['load_source_data'])
        #else:
            #return
            # upsert_data_from_stage_to_load_tables(pipeline_configuration['stage_source_data'], pipeline_configuration['load_source_data'])

        data_load_tracker.completed_successfully()
        self.logger.info(data_load_tracker.get_statistics())

    def remove_invalid_columns(self, source_table_configuration, column_configration, source_engine):
        source_table_manager = SourceTableManager()
        existing_columns = source_table_manager.get_columns(source_table_configuration, source_engine)
        return list(filter(lambda column: self.column_exists(column['source_name'], existing_columns), column_configration))

    def column_exists(self, column_name, existing_columns):
        if column_name in existing_columns:
            return True
        self.logger.warning("Column {0} does not exist in source. It will be ignored for now, however may cause downstream issues.".format(column_name))
        return False
