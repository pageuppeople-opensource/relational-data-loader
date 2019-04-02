import logging
import os.path
from pathlib import Path
import pandas

from rdl.ColumnTypeResolver import ColumnTypeResolver
from rdl.data_sources.ChangeTrackingInfo import ChangeTrackingInfo
from rdl.shared.Utils import prevent_senstive_data_logging


class CsvDataSource(object):
    def __init__(self, connection_string, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.source_path = Path(connection_string[len(self.connection_string_prefix()):])
        self.column_type_resolver = ColumnTypeResolver()

    @staticmethod
    def can_handle_connection_string(connection_string):
        return connection_string.startswith(CsvDataSource.connection_string_prefix())

    @staticmethod
    def connection_string_prefix():
        return 'csv://'

    def assert_data_source_is_valid(self, table_config, configured_columns):
        csv_file = os.path.abspath(self.source_path / f"{table_config['name']}.csv")
        self.logger.debug(f"Path to CSV file: '{csv_file}'")

        if not os.path.exists(csv_file):
            message = f"'{csv_file}' does not exist and will not be processed."
            raise ValueError(message)

        data_frame = pandas.read_csv(csv_file, nrows=1)

        for column in configured_columns:
            self.assert_column_exists(column['source_name'], data_frame, csv_file)

    def assert_column_exists(self, column_name, data_frame, csv_file):
        if column_name in data_frame.columns:
            return True

        message = f"Column '{column_name}' does not exist in source '{csv_file}'"
        raise ValueError(message)

    # For now, the CSV data sources will get all rows in the CSV regardless of
    # batch size. - Ie, they don't currently support paging.
    @prevent_senstive_data_logging
    def get_next_data_frame(
            self,
            table_config,
            columns,
            batch_config,
            batch_tracker,
            batch_key_tracker,
            full_refresh,
            change_tracking_info):

        # There is no incremental loading in CSV - therefore, we will check if we have loaded data before in that run
        # if we have, we have loaded all the data.
        if batch_key_tracker.bookmarks[batch_key_tracker.primary_keys[0]] > 0:
            return None

        csv_file = os.path.abspath(self.source_path / f"{table_config['name']}.csv")
        self.logger.debug(f"Path to CSV file: '{csv_file}'")

        if not os.path.exists(csv_file):
            self.logger.warning(f"'{csv_file}' does not exist. Returning a None dataframe.")
            return None

        self.logger.debug(f"Starting read of file: '{csv_file}'")

        data_frame = pandas.read_csv(csv_file)
        self.logger.debug("Completed read")

        batch_tracker.extract_completed_successfully(len(data_frame))
        return data_frame

    def init_change_tracking(self, table_config, last_sync_version):
        return ChangeTrackingInfo(0, 0, False)
