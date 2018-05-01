import logging
import pandas
from io import StringIO
import importlib





class BatchDataLoader(object):
    def __init__(self, source_table_configuration, columns, batch_configuration, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.source_table_configuration = source_table_configuration
        self.columns = columns
        self.batch_configuration = batch_configuration

    def build_select_statement(self, previous_key=0):

        column_array = list(map(lambda cfg: cfg['source_name'], self.columns))
        column_names = ", ".join(column_array)


        return "SELECT TOP ({0}) {1} FROM {2}.{3} WHERE {4} > {5} ORDER BY {4}".format(self.batch_configuration['size'],
                                                            column_names,
                                                            self.source_table_configuration['schema'],
                                                            self.source_table_configuration['name'],
                                                            self.batch_configuration['source_unique_column'],
                                                            previous_key
                                                            )

    # Imports rows, returns True if >0 rows were found
    def import_batch(self, source_engine, target_engine, target_table_configuration, batch_tracker, previous_key):
        self.logger.debug("ImportBatch Starting for source {0} target {1} previous_key {2}".format(self.source_table_configuration['name'],
                                                                                                   target_table_configuration['name'],
                                                                                                   previous_key))
        sql = self.build_select_statement(previous_key)
        self.logger.debug("SQL Statement: {0}".format(sql))
        self.logger.info("Starting read")
        data_frame = pandas.read_sql_query(sql, source_engine)
        self.logger.info("Completed read")

        batch_tracker.extract_completed_successfully(len(data_frame))

        self.attach_column_transformers(data_frame)

        if len(data_frame) == 0:
            self.logger.info("There are no rows to import, returning False")
            batch_tracker.load_skipped_due_to_zero_rows()
            return -1

        self.write_data_frame_to_table(data_frame, target_table_configuration, target_engine)
        batch_tracker.load_completed_successfully()

        last_key_returned = data_frame.iloc[-1][self.batch_configuration['source_unique_column']]

        self.logger.debug("Returning {0} to signify we loaded data.".format(last_key_returned))

        return last_key_returned

    def write_data_frame_to_table(self, data_frame, table_configuration, target_engine):
        destination_table = "{0}.{1}".format(table_configuration['schema'], table_configuration['name'])
        self.logger.info("Starting write to table {0}".format(destination_table))
        data = StringIO()
        data_frame.to_csv(data, header=False, index=False, na_rep='')
        data.seek(0)
        raw = target_engine.raw_connection()
        curs = raw.cursor()

        column_array = list(map(lambda cfg: cfg['destination']['name'], self.columns))

        curs.copy_from(data, destination_table, sep=',', columns=column_array, null='')
        self.logger.info("Completed write to table {0}".format(destination_table))

        curs.connection.commit()
        return

    def attach_column_transformers(self, data_frame):
        return
        #for column in self.columns:
            #if 'column_transformer' in column:

                #TODO: this is horribly broken
                #data_frame = data_frame[column['source_name']].map(lambda x: x.upper())
                #print (data_frame)
                #transformer = self.create_column_transformer_type(column['column_transformer'])
               #// df['a'] = df['a'].map(lambda a: a / 2.)

                #data_frame.




    def create_column_transformer_type(self, type_name):
        module = importlib.import_module(type_name)
        class_ = getattr(module, type_name)
        instance = class_()
        return instance

    def remove_non_existent_columns(self, columns):
        pass