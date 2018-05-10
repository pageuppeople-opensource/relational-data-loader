import logging
from lib.BatchDataLoader import BatchDataLoader
from lib.DestinationTableManager import DestinationTableManager
from lib.DataLoadTracker import DataLoadTracker
import os
import json


class DataLoadManager(object):
    def __init__(self, configuration_path, data_source, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.configuration_path = configuration_path
        self.data_source = data_source

    def start_imports(self, target_engine, full_refresh):
        for file in os.listdir(self.configuration_path):
            self.start_single_import(target_engine, file, full_refresh)

        self.logger.info("Execution completed.")

    def start_single_import(self, target_engine, configuration_name, requested_full_refresh):
        self.logger.debug("Using configuration file : {0}".format(configuration_name))

        config_file = os.path.abspath(self.configuration_path + configuration_name)
        self.logger.debug("Using configuration file : {0}".format(config_file))
        with open(config_file) as json_data:
            pipeline_configuration = json.load(json_data)

        self.logger.info("Execute Starting for: {0} requested_full_refresh: {1}".format(configuration_name, requested_full_refresh))

        destination_table_manager = DestinationTableManager(target_engine)

        full_refresh = requested_full_refresh
        if not requested_full_refresh and not destination_table_manager.table_exists(pipeline_configuration['target_schema'],
                                                                                     pipeline_configuration['load_table']):
            self.logger.warning("The load table {0}.{1} does not exist. Swapping to full-refresh mode".format(pipeline_configuration['target_schema'],
                                                                                                              pipeline_configuration['load_table']))
            full_refresh = True

        data_load_tracker = DataLoadTracker(configuration_name, json_data, full_refresh)

        columns = self.data_source.get_valid_columns(pipeline_configuration['source_table'],
                                                     pipeline_configuration['columns'])

        if columns is None:
            self.logger.debug("There are no columns, returning.")
            return

        destination_table_manager.create_schema(pipeline_configuration['target_schema'])

        self.logger.info("Recreating the staging table {0}.{1}".format(pipeline_configuration['target_schema'], pipeline_configuration['stage_table']))
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

        if full_refresh:
            # Rename the stage table to the load table.
            self.logger.info("Full-load is set. Renaming the stage table to the load table.")
            destination_table_manager.rename_table(pipeline_configuration['target_schema'],
                                                   pipeline_configuration['stage_table'],
                                                   pipeline_configuration['load_table'])
        else:
            self.logger.info("Incremental-load is set. Upserting from the stage table to the load table.")
            destination_table_manager.upsert_table(pipeline_configuration['target_schema'],
                                                   pipeline_configuration['stage_table'],
                                                   pipeline_configuration['load_table'],
                                                   pipeline_configuration['columns'])

            destination_table_manager.drop_table(pipeline_configuration['target_schema'],
                                                 pipeline_configuration['stage_table'])
        data_load_tracker.completed_successfully()
        self.logger.info("Import for configuration: {0} Complete. {1}".format(configuration_name, data_load_tracker.get_statistics()))

