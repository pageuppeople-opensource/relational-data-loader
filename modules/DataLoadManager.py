import os
import json
import uuid
import logging
import hashlib
from pathlib import Path
from json import JSONDecodeError
from modules.BatchDataLoader import BatchDataLoader
from modules.DestinationTableManager import DestinationTableManager
from modules.data_load_tracking.DataLoadTracker import DataLoadTracker
from modules.BatchKeyTracker import BatchKeyTracker
from modules.Shared import Constants


class DataLoadManager(object):
    def __init__(self, configuration_path, source_db, destination_db, data_load_tracker_repository, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.configuration_path = configuration_path
        self.source_db = source_db
        self.destination_db = destination_db
        self.data_load_tracker_repository = data_load_tracker_repository
        self.correlation_id = uuid.uuid4()
        self.model_pattern = '**/{model_name}.json'
        self.all_model_pattern = self.model_pattern.format(model_name='*')

    def start_imports(self, force_full_refresh_models):
        model_folder = Path(self.configuration_path)
        if not model_folder.is_dir():
            raise NotADirectoryError(self.configuration_path)

        force_full_refresh_all_models = force_full_refresh_models == '*'
        force_full_refresh_model_list = []
        if force_full_refresh_models is not None:
            force_full_refresh_model_list = force_full_refresh_models.split(',')
        all_model_files = {}
        for model_file in model_folder.glob(self.all_model_pattern):
            all_model_files[model_file.stem] = (model_file, force_full_refresh_all_models)

        if not force_full_refresh_all_models:
            for model_name in force_full_refresh_model_list:
                named_model_pattern = self.model_pattern.format(model_name=model_name)
                model_file_objs = [model_file for model_file in model_folder.glob(named_model_pattern)]
                if len(model_file_objs) == 0:
                    raise FileNotFoundError(f"'{named_model_pattern}' does not exist in '{self.configuration_path}'")
                if len(model_file_objs) > 1:
                    raise KeyError(f"Multiple models with name '{model_name}' exist in '{self.configuration_path}'")
                model_file = model_file_objs[0]
                all_model_files[model_file.stem] = (model_file, True)

        for (model_file, request_full_refresh) in all_model_files.values():
            self.start_single_import(model_file, request_full_refresh)

        self.logger.info("Execution completed.")

    def start_single_import(self, model_file, requested_full_refresh):
        model_name = model_file.stem
        self.logger.debug(f"Model name: {model_name}")

        self.logger.info(f"Processing model: {model_name}, requested_full_refresh: {requested_full_refresh}")

        model_file_full_path = str(model_file.absolute().resolve())
        self.logger.debug(f"Model file: {model_file_full_path}")

        try:
            self.logger.debug(f"Starting to read model: '{model_file_full_path}'")
            with open(model_file_full_path) as model_file:
                model_checksum = hashlib.md5(model_file.read().encode('utf-8')).hexdigest()
                model_file.seek(0)
                pipeline_configuration = json.load(model_file)
                self.logger.debug(f"Finished reading model file: '{model_file_full_path}'")
            pass
        except JSONDecodeError as exception:
            self.logger.error(f"Failed to read model file '{model_file_full_path}' with error: '{str(exception)}'")
            raise exception

        self.source_db.assert_data_source_is_valid(pipeline_configuration['source_table'],
                                                   pipeline_configuration['columns'])

        last_sync_version = 0
        last_successful_data_load_execution = self.data_load_tracker_repository.get_last_successful_data_load_execution(
            model_name)

        if last_successful_data_load_execution is not None:
            last_sync_version = last_successful_data_load_execution.next_sync_version

        destination_table_manager = DestinationTableManager(self.destination_db)
        change_tracking_info = self.source_db.init_change_tracking(pipeline_configuration['source_table'],
                                                                   last_sync_version)

        full_refresh_reason, full_refresh = DataLoadManager.is_full_refresh(
            user_requested=requested_full_refresh,
            destination_table_exists=destination_table_manager.table_exists(
                pipeline_configuration['target_schema'],
                pipeline_configuration['load_table']),
            last_successful_execution_exists=last_successful_data_load_execution is not None,
            model_changed=last_successful_data_load_execution.model_checksum != model_checksum,
            invalid_change_tracking=change_tracking_info.force_full_load
        )

        if full_refresh:
            self.logger.info(f"Performing full refresh for reason '{full_refresh_reason}'")

        data_load_tracker = DataLoadTracker(model_name, model_checksum, model_file, full_refresh, change_tracking_info,
                                            self.correlation_id, full_refresh_reason)

        columns = pipeline_configuration['columns']
        destination_table_manager.create_schema(pipeline_configuration['target_schema'])

        self.logger.debug(f"Recreating the staging table {pipeline_configuration['target_schema']}."
                          f"{pipeline_configuration['stage_table']}")
        destination_table_manager.create_table(pipeline_configuration['target_schema'],
                                               pipeline_configuration['stage_table'],
                                               columns, drop_first=True)

        # Import the data.
        batch_data_loader = BatchDataLoader(self.source_db,
                                            pipeline_configuration['source_table'],
                                            pipeline_configuration['target_schema'],
                                            pipeline_configuration['stage_table'],
                                            columns,
                                            data_load_tracker,
                                            pipeline_configuration['batch'],
                                            self.destination_db,
                                            full_refresh,
                                            change_tracking_info)

        batch_key_tracker = BatchKeyTracker(pipeline_configuration['source_table']['primary_keys'])
        while batch_key_tracker.has_more_data:
            batch_data_loader.load_batch(batch_key_tracker)

        if full_refresh:
            # Rename the stage table to the load table.
            self.logger.debug("Full-load is set. Renaming the stage table to the load table.")
            destination_table_manager.rename_table(pipeline_configuration['target_schema'],
                                                   pipeline_configuration['stage_table'],
                                                   pipeline_configuration['load_table'])
        else:
            self.logger.debug("Incremental-load is set. Upserting from the stage table to the load table.")
            destination_table_manager.upsert_table(pipeline_configuration['target_schema'],
                                                   pipeline_configuration['stage_table'],
                                                   pipeline_configuration['load_table'],
                                                   pipeline_configuration['columns'])

            destination_table_manager.drop_table(pipeline_configuration['target_schema'],
                                                 pipeline_configuration['stage_table'])
        data_load_tracker.completed_successfully()
        self.data_load_tracker_repository.save(data_load_tracker)
        self.logger.info(f"Import Complete for: {model_name}. {data_load_tracker.get_statistics()}")

    @staticmethod
    def is_full_refresh(*,
                        user_requested,
                        destination_table_exists,
                        last_successful_execution_exists,
                        model_changed,
                        invalid_change_tracking):

        if user_requested:
            return Constants.FullRefreshReason.USER_REQUESTED, True

        if not destination_table_exists:
            return Constants.FullRefreshReason.DESTINATION_TABLE_ABSENT, True

        if not last_successful_execution_exists:
            return Constants.FullRefreshReason.FIRST_EXECUTION, True

        if last_successful_execution_exists and model_changed:
            return Constants.FullRefreshReason.MODEL_CHANGED, True

        if invalid_change_tracking:
            return Constants.FullRefreshReason.INVALID_CHANGE_TRACKING, True

        return Constants.FullRefreshReason.NOT_APPLICABLE, False
