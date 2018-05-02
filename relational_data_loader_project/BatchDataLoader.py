import logging
from io import StringIO
import importlib

from column_transformers.StringTransformers import ToUpper


class BatchDataLoader(object):
    def __init__(self, data_source, source_table_configuration, target_table_configuration, columns, data_load_tracker, batch_configuration, target_engine, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.source_table_configuration = source_table_configuration
        self.columns = columns
        self.data_source = data_source
        self.target_table_configuration = target_table_configuration
        self.data_load_tracker = data_load_tracker
        self.batch_configuration = batch_configuration
        self.target_engine = target_engine

    # Imports rows, returns True if >0 rows were found
    def import_batch(self, previous_batch_key):
        batch_tracker = self.data_load_tracker.start_batch()

        self.logger.debug("ImportBatch Starting from previous_batch_key: {0}".format(previous_batch_key))

        data_frame = self.data_source.get_next_data_frame(self.source_table_configuration, self.columns, self.batch_configuration, batch_tracker, previous_batch_key)

        if len(data_frame) == 0:
            self.logger.debug("There are no rows to import, returning -1")
            batch_tracker.load_skipped_due_to_zero_rows()
            return -1

        data_frame = self.attach_column_transformers(data_frame)

        self.write_data_frame_to_table(data_frame, self.target_table_configuration, self.target_engine)
        batch_tracker.load_completed_successfully()

        last_key_returned = data_frame.iloc[-1][self.batch_configuration['source_unique_column']]

        self.logger.info("Batch key {0} Completed. {1}".format(last_key_returned, batch_tracker.get_statistics()))
        return last_key_returned

    def write_data_frame_to_table(self, data_frame, table_configuration, target_engine):
        destination_table = "{0}.{1}".format(table_configuration['schema'], table_configuration['name'])
        self.logger.debug("Starting write to table {0}".format(destination_table))
        data = StringIO()
        data_frame.to_csv(data, header=False, index=False, na_rep='')
        data.seek(0)
        raw = target_engine.raw_connection()
        curs = raw.cursor()

        #TODO: This is assuming that our destination schema column order matches the columns in the dataframe. This
        #isn't always correct (especially in csv sources) - therefore, we should derive the column_array from the
        #data frames' columns.
        column_array = list(map(lambda cfg: cfg['destination']['name'], self.columns))

        curs.copy_from(data, destination_table, sep=',', columns=column_array, null='')
        self.logger.debug("Completed write to table {0}".format(destination_table))

        curs.connection.commit()
        return

    def attach_column_transformers(self, data_frame):
        self.logger.debug("Attaching column transformers")
        for column in self.columns:
            if 'column_transformer' in column:
                # transformer = self.create_column_transformer_type(column['column_transformer'])
                transformer = ToUpper.execute;
                data_frame[column['source_name']] = data_frame[column['source_name']].map(transformer)
                # print (data_frame)
        return data_frame

    def create_column_transformer_type(self, type_name):
        module = importlib.import_module(type_name)
        class_ = getattr(module, type_name)
        instance = class_()
        return instance
