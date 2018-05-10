from datetime import datetime


class DataLoadTracker:
    started = None
    completed = None
    status = "Not Started"
    total_row_count = 0
    batches = []
    configuration_name = None
    configuration = None
    is_full_load = False
    total_execution_time = None
    total_row_count = 0
    rows_per_second = 0

    def __init__(self, configuration_name, configuration, is_full_load):
        self.configuration_name = configuration_name
        self.configuration = configuration
        self.is_full_load = is_full_load
        self.started = datetime.now()
        self.status = "Not Started"

    def start_batch(self):
        batch = self.Batch()
        self.batches.append(batch)
        return batch

    def completed_successfully(self):
        self.completed = datetime.now()
        self.total_execution_time = self.completed - self.started

        for batch in self.batches:
            self.total_row_count = self.total_row_count + batch.row_count

        self.rows_per_second = self.total_row_count / self.total_execution_time.total_seconds()

    def get_statistics(self):
        return "Rows: {0}, Total Execution Time: {1}. ({2:.2f} rows per second)".format(self.total_row_count,
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
            #Add a tiny bit of time to guarentee against div by 0 errors.
            self.extract_rows_per_second = self.row_count / self.extract_execution_time.total_seconds() + 0.001

        def load_completed_successfully(self):
            self.status = "Load Completed Successfully"
            self.load_completed = datetime.now()
            self.load_execution_time = self.load_completed - self.extract_completed
            self.load_rows_per_second = self.row_count / self.load_execution_time.total_seconds()
            self.total_execution_time = self.load_completed - self.extract_started
            # Add a tiny bit of time to guarentee against div by 0 errors.
            self.total_rows_per_second = self.row_count / self.total_execution_time.total_seconds() + 0.001

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
