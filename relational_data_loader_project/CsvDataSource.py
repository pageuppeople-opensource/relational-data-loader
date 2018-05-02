import logging
import pandas


class CsvDataSource(object):
    def __init__(self, source_path, source_table_configuration, columns, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.source_path = source_path
        self.columns = columns

    def get_data_frame(self, batch_tracker, previous_key=0):
        path_to_csv_file = "{0}{1}.csv".format(self.source_path, self.source_table_configuration['source_table']['name'])

        self.logger.debug("Starting read of file: {0}".format(path_to_csv_file))
        data_frame = pandas.read_csv(path_to_csv_file)
        self.logger.debug("Completed read")

        batch_tracker.extract_completed_successfully(len(data_frame))
        return data_frame


