from datetime import datetime


class DataLoadTracker:
    started = None
    completed = None
    status = "Not Started"
    total_row_count = 0
    batches = []
    model_name = None
    configuration = None
    is_full_refresh = False
    total_execution_time = None
    total_row_count = 0
    rows_per_second = 0
    correlation_id = None,

    def __init__(self, model_name, configuration, is_full_refresh, change_tracking_info, correlation_id):
        self.model_name = model_name
        self.configuration = configuration
        self.is_full_refresh = is_full_refresh
        self.started = datetime.now()
        self.status = "Not Started"
        self.change_tracking_info = change_tracking_info
        self.correlation_id = correlation_id

    def start_batch(self):
        batch = self.Batch()
        self.batches.append(batch)
        return batch

    def completed_successfully(self):
        self.completed = datetime.now()
        self.total_execution_time = self.completed - self.started
        self.status = "Completed Successfully"
        for batch in self.batches:
            self.total_row_count = self.total_row_count + batch.row_count

        self.rows_per_second = self.total_row_count / self.total_execution_time.total_seconds()

    def get_statistics(self):
        load_type = 'full' if self.is_full_refresh else "incremental from version {0} ".format(self.change_tracking_info.this_sync_version)
        return "Rows: {0} ({1}), Total Execution Time: {2}. ({3:.2f} rows per second) ".format(self.total_row_count,
                                                                                               load_type,
                                                                                               self.total_execution_time,
                                                                                               self.rows_per_second)

    class Batch:
        row_count = 0
        extract_started = None
        extract_completed = None
        load_completed = None
        status = "Not Started"

        extract_execution_time = None
        extract_rows_per_second = 0
        load_execution_time = None
        load_rows_per_second = 0
        total_rows_per_second = 0
        total_execution_time = None

        def __init__(self):
            self.extract_started = datetime.now()
            self.status = "Not Started"

        def extract_completed_successfully(self, row_count):
            self.status = "Extract Completed Successfully"
            self.row_count = row_count
            self.extract_completed = datetime.now()
            self.extract_execution_time = self.extract_completed - self.extract_started
            if self.extract_execution_time.total_seconds() == 0:
                self.extract_rows_per_second = self.row_count
            else:
                self.extract_rows_per_second = self.row_count / self.extract_execution_time.total_seconds()

        def load_completed_successfully(self):
            self.status = "Load Completed Successfully"
            self.load_completed = datetime.now()
            self.load_execution_time = self.load_completed - self.extract_completed

            self.total_execution_time = self.load_completed - self.extract_started

            if self.total_execution_time.total_seconds() == 0:
                self.total_rows_per_second = self.row_count
            else:
                self.total_rows_per_second = self.row_count / self.total_execution_time.total_seconds()

            if self.load_execution_time.total_seconds() == 0:
                self.load_rows_per_second = self.row_count
            else:
                self.load_rows_per_second = self.row_count / self.load_execution_time.total_seconds()


        def load_skipped_due_to_zero_rows(self):
            self.status = "Skipped - Zero Rows"
            self.load_completed = datetime.now()

        def get_statistics(self):
            return "Rows: {0}, Extract Execution Time: {1} ({2:.2f} rows per second). Load Execution Time {3} ({4:.2f} rows per second) Total Execution Time {5} ({6:.2f} rows per second)".format(self.row_count,
                                                                                       self.extract_execution_time,
                                                                                       self.extract_rows_per_second,
                                                                                       self.load_execution_time,
                                                                                       self.load_rows_per_second,
                                                                                       self.total_execution_time,
                                                                                       self.total_rows_per_second)
