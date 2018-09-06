import os
import json
import uuid
import logging
import hashlib

from json import JSONDecodeError
from modules.BatchDataLoader import BatchDataLoader
from modules.DestinationTableManager import DestinationTableManager
from modules.data_load_tracking.DataLoadTracker import DataLoadTracker


class DataLoadManager(object):
    def __init__(self, configuration_path, data_source, data_load_tracker_repository, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.configuration_path = configuration_path
        self.data_source = data_source
        self.data_load_tracker_repository = data_load_tracker_repository
        self.correlation_id = uuid.uuid4()

    def start_imports(self, target_engine, full_refresh):
        for file in os.listdir(self.configuration_path):
            self.start_single_import(target_engine, file, full_refresh)

        self.logger.info("Execution completed.")

    def start_single_import(self, target_engine, model_name, requested_full_refresh):
        self.logger.debug(f"Using configuration file : {model_name}")

        config_file = os.path.abspath(self.configuration_path + model_name)
        self.logger.debug(f"Using configuration file : {config_file}")

        try:
            self.logger.info(f"Parsing started for: '{model_name}'")
            with open(config_file) as json_file:
                model_checksum = hashlib.md5(json_file.read().encode('utf-8')).hexdigest()
                json_file.seek(0)
                pipeline_configuration = json.load(json_file)
            pass

        except JSONDecodeError:
            self.logger.info(f"Parsing failed for: '{model_name}'")
            raise

        self.logger.info(f"Execute Starting for: {model_name} requested_full_refresh: {requested_full_refresh}")

        destination_table_manager = DestinationTableManager(target_engine)

        full_refresh_reason = "Command Line Argument" if requested_full_refresh else "N/A"
        full_refresh = requested_full_refresh
        if not requested_full_refresh and not destination_table_manager.table_exists(
                pipeline_configuration['target_schema'],
                pipeline_configuration['load_table']):
            self.logger.warning(f"The load table {pipeline_configuration['target_schema']}. "
                                f"{pipeline_configuration['load_table']} does not exist. Swapping to full-refresh mode")

            full_refresh_reason = "Destination table does not exist"
            full_refresh = True

        self.data_source.assert_data_source_is_valid(pipeline_configuration['source_table'],
                                                     pipeline_configuration['columns'])

        last_successful_data_load_execution = self.data_load_tracker_repository.get_last_successful_data_load_execution(
            model_name)

        if last_successful_data_load_execution is None:
            last_sync_version = 0
            full_refresh_reason = "First Execution"
            full_refresh = True
        else:
            self.logger.debug(
                f"Previous Checksum {last_successful_data_load_execution.model_checksum}. "
                f"Current Checksum {model_checksum}")
            last_sync_version = last_successful_data_load_execution.next_sync_version
            if not full_refresh and last_successful_data_load_execution.model_checksum != model_checksum:
                self.logger.info("A model checksum change has forced this to be a full load")
                full_refresh = True
                full_refresh_reason = "Model Change"

        change_tracking_info = self.data_source.init_change_tracking(pipeline_configuration['source_table'],
                                                                     last_sync_version)

        if not full_refresh and change_tracking_info.force_full_load:
            self.logger.info("Change tracking has forced this to be a full load")
            full_refresh = True
            full_refresh_reason = "Change Tracking Invalid"

        data_load_tracker = DataLoadTracker(model_name, model_checksum, json_file, full_refresh, change_tracking_info,
                                            self.correlation_id, full_refresh_reason)

        columns = pipeline_configuration['columns']
        destination_table_manager.create_schema(pipeline_configuration['target_schema'])

        self.logger.debug(f"Recreating the staging table {pipeline_configuration['target_schema']}."
                          f"{pipeline_configuration['stage_table']}")
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
        self.data_load_tracker_repository.save(data_load_tracker)
        self.logger.info(f"Import Complete for: {model_name}. {data_load_tracker.get_statistics()}")
