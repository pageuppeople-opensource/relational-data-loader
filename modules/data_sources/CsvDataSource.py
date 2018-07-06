import logging
import pandas
import os.path
from modules.ColumnTypeResolver import ColumnTypeResolver
from pathlib import Path
from modules.data_sources.ChangeTrackingInfo import ChangeTrackingInfo


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

    def assert_data_source_is_valid(self, table_configuration, configured_columns):
        csv_file = os.path.abspath(self.source_path / "{0}.csv".format(table_configuration['name']))
        self.logger.debug("Path to CSV file: {0}.".format(csv_file))

        if not os.path.exists(csv_file):
            message = "{0} does not exist and will not be processed.".format(csv_file)
            raise ValueError(message)

        data_frame = pandas.read_csv(csv_file, nrows=1)

        for column in configured_columns:
            self.assert_column_exists(column['source_name'], data_frame, csv_file)


    def assert_column_exists(self, column_name, data_frame, csv_file):
        if column_name in data_frame.columns:
            return True

        message = 'Column {0} does not exist in source {1}'.format(column_name, csv_file)
        raise ValueError(message)


    # For now, the CSV data sources will get all rows in the CSV regardless of batch size. - Ie, they don't currently support paging.
    def get_next_data_frame(self, table_configuration, columns, batch_configuration, batch_tracker, previous_batch_key, full_refresh, change_tracking_info):

        if previous_batch_key > 0:
            return None

        csv_file = os.path.abspath(self.source_path / "{0}.csv".format(table_configuration['name']))
        self.logger.debug("Path to CSV file: {0}.".format(csv_file))

        if not os.path.exists(csv_file):
            self.logger.warning("{0} does not exist. Returning a None dataframe.".format(csv_file))
            return None

        self.logger.debug("Starting read of file: {0}".format(csv_file))

        data_frame = pandas.read_csv(csv_file)
        self.logger.debug("Completed read")

        batch_tracker.extract_completed_successfully(len(data_frame))
        return data_frame

    def init_change_tracking(self, table_configuration, last_sync_version):
        return ChangeTrackingInfo(0,0)
