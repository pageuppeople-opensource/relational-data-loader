import os
import json
import logging
from modules.BatchDataLoader import BatchDataLoader
from modules.DestinationTableManager import DestinationTableManager
from modules.DataLoadTracker import DataLoadTracker



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

        self.data_source.assert_data_source_is_valid(pipeline_configuration['source_table'],
                                                     pipeline_configuration['columns'])

        last_sync_version = destination_table_manager.get_last_sync_version(pipeline_configuration['target_schema'],
                                                                            pipeline_configuration['load_table'])

        change_tracking_info = self.data_source.init_change_tracking(pipeline_configuration['source_table'],
                                                                     last_sync_version)


        data_load_tracker = DataLoadTracker(configuration_name, json_data, full_refresh, change_tracking_info)


        self.logger.debug(" Change Tracking: this_sync_version: {0} next_sync_version: {1} force_full_load:{2}    : ".format(change_tracking_info.this_sync_version, change_tracking_info.next_sync_version, change_tracking_info.force_full_load()))
        if not full_refresh and change_tracking_info.force_full_load():
            self.logger.info("Change tracking has forced this to be a full load")
            full_refresh = True

        columns = pipeline_configuration['columns']
        destination_table_manager.create_schema(pipeline_configuration['target_schema'])

        self.logger.debug("Recreating the staging table {0}.{1}".format(pipeline_configuration['target_schema'], pipeline_configuration['stage_table']))
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
                                         target_engine,
                                         full_refresh,
                                         change_tracking_info)

        previous_unique_column_value = 0
        while previous_unique_column_value > -1:
            previous_unique_column_value = batch_data_loader.load_batch(previous_unique_column_value)

        if full_refresh:
            # Rename the stage table to the load table.
            self.logger.debug("Full-load is set. Renaming the stage table to the load table.")
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
        self.logger.info("Import Complete for: {0}. {1}".format(configuration_name, data_load_tracker.get_statistics()))

