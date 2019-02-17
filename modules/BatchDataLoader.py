import logging
from io import StringIO
from modules.column_transformers.StringTransformers import ToUpper


class BatchDataLoader(object):
    def __init__(self, source_db, source_table_config, target_schema, target_table, columns, data_load_tracker,
                 batch_config, target_db, full_refresh, change_tracking_info, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.source_table_config = source_table_config
        self.columns = columns
        self.source_db = source_db
        self.target_schema = target_schema
        self.target_table = target_table
        self.data_load_tracker = data_load_tracker
        self.batch_config = batch_config
        self.target_db = target_db
        self.full_refresh = full_refresh
        self.change_tracking_info = change_tracking_info

    # Imports rows, returns True if >0 rows were found
    def load_batch(self, batch_key_tracker):
        batch_tracker = self.data_load_tracker.start_batch()

        self.logger.debug(f"ImportBatch Starting from previous_batch_key: '{batch_key_tracker.bookmarks}'. "
                          f"Full Refresh: '{self.full_refresh}' "
                          f"this_sync_version: '{self.change_tracking_info.this_sync_version}'")

        data_frame = self.source_db.get_next_data_frame(self.source_table_config, self.columns,
                                                        self.batch_config, batch_tracker, batch_key_tracker,
                                                        self.full_refresh, self.change_tracking_info)

        if data_frame is None or len(data_frame) == 0:
            self.logger.debug("There are no more rows to import.")
            batch_tracker.load_skipped_due_to_zero_rows()
            batch_key_tracker.has_more_data = False
            return

        data_frame = self.attach_column_transformers(data_frame)
        self.write_data_frame_to_table(data_frame)
        batch_tracker.load_completed_successfully()

        for primary_key in batch_key_tracker.primary_keys:
            batch_key_tracker.set_bookmark(primary_key, data_frame.iloc[-1][primary_key])

        self.logger.info(f"Batch keys '{batch_key_tracker.bookmarks}' completed. {batch_tracker.get_statistics()}")

    def write_data_frame_to_table(self, data_frame):
        qualified_target_table = f'{self.target_schema}.{self.target_table}'
        self.logger.debug(f"Starting write to table '{qualified_target_table}'")
        data = StringIO()
        data_frame.to_csv(data, header=False, index=False, na_rep='', float_format='%.16g')
        # Float_format is used to truncate any insignificant digits. Unfortunately it gives us an artificial limitation

        data.seek(0)
        raw = self.target_db.raw_connection()
        curs = raw.cursor()

        column_array = list(
            map(lambda source_colum_name: self.get_destination_column_name(source_colum_name), data_frame.columns))
        column_list = ','.join(map(str, column_array))

        sql = f"COPY {qualified_target_table}({column_list}) FROM STDIN with csv"
        self.logger.debug(f"Writing to table using command '{sql}'")

        curs.copy_expert(sql=sql, file=data)

        self.logger.debug(f"Completed write to table '{qualified_target_table}'")

        curs.connection.commit()
        return

    def get_destination_column_name(self, source_column_name):
        for column in self.columns:
            if column['source_name'] == source_column_name:
                return column['destination']['name']

        # Internal columns - map them straight through
        if source_column_name.startswith("data_pipeline_"):
            return source_column_name

        message = f"A source column with name '{source_column_name}' was not found in the column configuration"
        raise ValueError(message)

    def attach_column_transformers(self, data_frame):
        self.logger.debug("Attaching column transformers")
        for column in self.columns:
            if 'column_transformer' in column:
                # transformer = Utils.create_type_instance(column['column_transformer'])
                transformer = ToUpper.execute
                data_frame[column['source_name']] = data_frame[column['source_name']].map(transformer)
                # print (data_frame)
        return data_frame
