import logging
import pandas
import os.path
from modules.ColumnTypeResolver import ColumnTypeResolver
from pathlib import Path


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

    # Normally, this returns an array of configured_columns containing only columns that this data source supports. Logs invalid ones.
    # However, for CSV files, we will raise an error when we find a configured column that does not exist, as the use-case around
    # CSV files is loading test data.
    def get_valid_columns(self, table_configuration, configured_columns):
        csv_file = os.path.abspath(self.source_path / "{0}.csv".format(table_configuration['name']))
        self.logger.debug("Path to CSV file: {0}.".format(csv_file))

        if not os.path.exists(csv_file):
            self.logger.warning("{0} does not exist and will not be processed.".format(csv_file))
            return None

        data_frame = pandas.read_csv(csv_file, nrows=1)

        return list(
            filter(lambda column: self.column_exists(column['source_name'], data_frame, csv_file), configured_columns))

    def column_exists(self, column_name, data_frame, csv_file):
        if column_name in data_frame.columns:
            return True
        self.logger.warning(
            "Column {0} does not exist in source {1}. It will be ignored for now, however may cause downstream issues.".format(
                column_name, csv_file))
        return False

    # For now, the CSV data sources will get all rows in the CSV regardless of batch size. - Ie, they don't currently support paging.
    def get_next_data_frame(self, table_configuration, columns, batch_configuration, batch_tracker, previous_batch_key):

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
