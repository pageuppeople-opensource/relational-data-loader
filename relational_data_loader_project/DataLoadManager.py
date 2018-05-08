import logging
from relational_data_loader_project.BatchDataLoader import BatchDataLoader
from relational_data_loader_project.DestinationTableManager import DestinationTableManager
from relational_data_loader_project.DataLoadTracker import DataLoadTracker
import os
import json


class DataLoadManager(object):
    def __init__(self, configuration_path, data_source, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.configuration_path = configuration_path
        self.data_source = data_source

    def start_imports(self, target_engine, full_load):
        for file in os.listdir(self.configuration_path):
            self.start_single_import(target_engine, file, full_load)

    def start_single_import(self, target_engine, configuration_name, full_load):

        with open("{0}{1}".format(self.configuration_path, configuration_name)) as json_data:
            pipeline_configuration = json.load(json_data)

        data_load_tracker = DataLoadTracker(configuration_name, json_data, full_load)

        self.logger.debug("Execute Starting")

        destination_table_manager = DestinationTableManager(target_engine)

        columns = self.data_source.get_valid_columns(pipeline_configuration['source_table'],
                                                     pipeline_configuration['columns'])

        destination_table_manager.create_schema(pipeline_configuration['target_schema'])
        if full_load:
            self.logger.info("Full-load is set. Recreating the staging table.")
            destination_table_manager.create_table(pipeline_configuration['target_schema'],
                                                   pipeline_configuration['stage_table'],
                                                   columns, drop_first=True)

        # Import the data.
        batch_data_loader = BatchDataLoader(self.data_source,
                                         pipeline_configuration['source_table'],
                                         pipeline_configuration['target_schema'],
                                         pipeline_configuration['stage_table'],
                                         columns,
                                         data_load_tracker,
                                         pipeline_configuration['batch'],
                                         target_engine)

        previous_unique_column_value = 0
        while previous_unique_column_value > -1:
            previous_unique_column_value = batch_data_loader.load_batch(previous_unique_column_value)

        self.logger.info("ImportBatch Completed")

        if full_load:
            #return
            # Rename the stage table to the load table.
            self.logger.info("Full-load is set. Renaming the stage table to the load table.")
            destination_table_manager.rename_table(pipeline_configuration['target_schema'],
                                                   pipeline_configuration['stage_table'],
                                                   pipeline_configuration['load_table'])
        #else:
            # upsert_data_from_stage_to_load_tables(pipeline_configuration['stage_source_data'], pipeline_configuration['load_source_data'])

        data_load_tracker.completed_successfully()
        self.logger.info(data_load_tracker.get_statistics())

