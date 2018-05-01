from datetime import datetime


class DataLoadTracker:
    started = datetime.now()
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
        return "Rows: {0}, Total Execution Time: {1}. ({2} rows per second)".format(self.total_row_count,
                                                                                    self.total_execution_time,
                                                                                    self.rows_per_second)

    class Batch:
        row_count = 0
        extract_started = datetime.now()
        extract_completed_on = None
        load_completed_on = None
        status = "Not Started"

        def __init__(self):
            pass

        def extract_completed_successfully(self, row_count):
            self.status = "Extract Completed Successfully"
            self.row_count = row_count
            self.extract_completed_on = datetime.now()

        def load_completed_successfully(self):
            self.status = "Load Completed Successfully"
            self.load_completed_on = datetime.now()

        def load_skipped_due_to_zero_rows(self):
            self.status = "Skipped - Zero Rows"
            self.load_completed_on = datetime.now()
