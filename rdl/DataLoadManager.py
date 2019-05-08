import json
import uuid
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from json import JSONDecodeError

from rdl.BatchDataLoader import BatchDataLoader
from rdl.DestinationTableManager import DestinationTableManager
from rdl.data_load_tracking.DataLoadTracker import DataLoadTracker
from rdl.BatchKeyTracker import BatchKeyTracker
from rdl.shared import Constants
from rdl.shared.Utils import SensitiveDataError


class DataLoadManager(object):
    def __init__(self, configuration_path, source_db, target_db, data_load_tracker_repository, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.configuration_path = configuration_path
        self.source_db = source_db
        self.target_db = target_db
        self.data_load_tracker_repository = data_load_tracker_repository
        self.model_pattern = '**/{model_name}.json'
        self.all_model_pattern = self.model_pattern.format(model_name='*')

    def start_imports(self, force_full_refresh_models):
        self.execution_id = self.data_load_tracker_repository.create_execution()

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

        all_model_names = sorted(list(all_model_files.keys()))
        total_number_of_models = len(all_model_names)
        model_number = 0
        for model_name in all_model_names:
            (model_file, request_full_refresh) = all_model_files[model_name]
            model_number += 1  # avoid all_model_names.index(model_name) due to linear time-complexity in list length
            self.start_single_import(model_file, request_full_refresh, model_number, total_number_of_models)

        return self.data_load_tracker_repository.complete_execution(self.execution_id, total_number_of_models)

    def start_single_import(self, model_file, requested_full_refresh, model_number, total_number_of_models):
        model_name = model_file.stem
        self.logger.debug(f"Model name: {model_name}")

        max_model_number_len = len(str(total_number_of_models))

        self.logger.info(f"{model_number:0{max_model_number_len}d} of {total_number_of_models}"
                         f" STARTING {model_name},"
                         f" requested_full_refresh={requested_full_refresh}")

        model_file_full_path = str(model_file.absolute().resolve())
        self.logger.debug(f"Model file: {model_file_full_path}")

        try:
            self.logger.debug(f"Starting to read model: '{model_file_full_path}'")
            with open(model_file_full_path) as model_file:
                model_checksum = hashlib.md5(model_file.read().encode('utf-8')).hexdigest()
                model_file.seek(0)
                model_config = json.load(model_file)
                self.logger.debug(f"Finished reading model file: '{model_file_full_path}'")
            pass
        except JSONDecodeError as exception:
            self.logger.error(f"Failed to read model file '{model_file_full_path}' with error: '{str(exception)}'")
            raise exception

        self.source_db.assert_data_source_is_valid(model_config['source_table'], model_config['columns'])

        last_sync_version = 0
        last_successful_data_load_execution = \
            self.data_load_tracker_repository.get_last_successful_data_load_execution(model_name)

        if last_successful_data_load_execution is not None:
            last_sync_version = last_successful_data_load_execution.sync_version

        destination_table_manager = DestinationTableManager(self.target_db)
        change_tracking_info = self.source_db.init_change_tracking(model_config['source_table'], last_sync_version)

        last_successful_execution_exists = last_successful_data_load_execution is not None
        model_changed = (not last_successful_execution_exists) or \
                        (last_successful_data_load_execution.model_checksum != model_checksum)

        full_refresh_reason, full_refresh = DataLoadManager.is_full_refresh(
            user_requested=requested_full_refresh,
            destination_table_exists=destination_table_manager.table_exists(
                model_config['target_schema'],
                model_config['load_table']),
            last_successful_execution_exists=last_successful_execution_exists,
            model_changed=model_changed,
            change_tracking_requests_full_load=change_tracking_info.force_full_load
        )

        if full_refresh:
            self.logger.info(f"Performing full refresh for reason '{full_refresh_reason}'")

        data_load_tracker = DataLoadTracker(self.execution_id, model_name, model_checksum, model_config,
                                            full_refresh, full_refresh_reason, change_tracking_info)
        self.data_load_tracker_repository.create_execution_model(data_load_tracker)
        destination_table_manager.create_schema(model_config['target_schema'])

        self.logger.debug(f"Recreating the staging table {model_config['target_schema']}."
                          f"{model_config['stage_table']}")
        destination_table_manager.create_table(model_config['target_schema'],
                                               model_config['stage_table'],
                                               model_config['columns'],
                                               drop_first=True)

        # Import the data.
        batch_data_loader = BatchDataLoader(self.source_db,
                                            model_config['source_table'],
                                            model_config['target_schema'],
                                            model_config['stage_table'],
                                            model_config['columns'],
                                            data_load_tracker,
                                            model_config['batch'],
                                            self.target_db,
                                            full_refresh,
                                            change_tracking_info)

        batch_key_tracker = BatchKeyTracker(model_config['source_table']['primary_keys'])
        while batch_key_tracker.has_more_data:
            try:
                batch_data_loader.load_batch(batch_key_tracker)
            except SensitiveDataError as e:
                data_load_tracker.data_load_failed(e.sensitive_error_args)
                self.data_load_tracker_repository.save_execution_model(data_load_tracker)
                self.data_load_tracker_repository.fail_execution(self.execution_id, model_number)
                raise e

        if full_refresh:
            # Rename the stage table to the load table.
            self.logger.debug("Full-load is set. Renaming the stage table to the load table.")
            destination_table_manager.rename_table(model_config['target_schema'],
                                                   model_config['stage_table'],
                                                   model_config['load_table'])
        else:
            self.logger.debug("Incremental-load is set. Upserting from the stage table to the load table.")
            destination_table_manager.upsert_table(model_config['target_schema'],
                                                   model_config['stage_table'],
                                                   model_config['load_table'],
                                                   model_config['columns'])

            destination_table_manager.drop_table(model_config['target_schema'],
                                                 model_config['stage_table'])
        data_load_tracker.data_load_successful()
        self.logger.info(f"{model_number:0{max_model_number_len}d} of {total_number_of_models}"
                         f" COMPLETED {model_name}")
        self.data_load_tracker_repository.save_execution_model(data_load_tracker)

    @staticmethod
    def is_full_refresh(*,
                        user_requested,
                        destination_table_exists,
                        last_successful_execution_exists,
                        model_changed,
                        change_tracking_requests_full_load):

        if user_requested:
            return Constants.FullRefreshReason.USER_REQUESTED, True

        if not destination_table_exists:
            return Constants.FullRefreshReason.DESTINATION_TABLE_ABSENT, True

        if not last_successful_execution_exists:
            return Constants.FullRefreshReason.FIRST_EXECUTION, True

        if last_successful_execution_exists and model_changed:
            return Constants.FullRefreshReason.MODEL_CHANGED, True

        if change_tracking_requests_full_load:
            return Constants.FullRefreshReason.INVALID_CHANGE_TRACKING, True

        return Constants.FullRefreshReason.NOT_APPLICABLE, False
