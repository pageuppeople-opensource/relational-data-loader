from datetime import datetime
from modules.Shared import Constants


class DataLoadTracker:
    started = None
    completed = None
    status = Constants.ExecutionStatus.NOT_STARTED
    total_row_count = 0
    batches = []
    model_name = None
    configuration = None
    is_full_refresh = False
    total_execution_time = None
    rows_per_second = 0
    correlation_id = None,
    full_refresh_reason = Constants.FullRefreshReason.NOT_APPLICABLE

    def __init__(
            self,
            model_name,
            model_checksum,
            configuration,
            is_full_refresh,
            change_tracking_info,
            correlation_id,
            full_refresh_reason):
        self.model_name = model_name
        self.model_checksum = model_checksum
        self.configuration = configuration
        self.is_full_refresh = is_full_refresh
        self.started = datetime.now()
        self.status = Constants.ExecutionStatus.NOT_STARTED
        self.change_tracking_info = change_tracking_info
        self.correlation_id = correlation_id
        self.full_refresh_reason = full_refresh_reason

    def start_batch(self):
        batch = self.Batch()
        self.batches.append(batch)
        return batch

    def completed_successfully(self):
        self.completed = datetime.now()
        self.total_execution_time = self.completed - self.started
        self.status = Constants.ExecutionStatus.COMPLETED_SUCCESSFULLY
        for batch in self.batches:
            self.total_row_count += batch.row_count

        self.rows_per_second = self.total_row_count / self.total_execution_time.total_seconds()

    def get_statistics(self):
        load_type = 'Full' if self.is_full_refresh else f"Incremental from " \
                                                        f"version '{self.change_tracking_info.this_sync_version}'"
        return f"Rows: {self.total_row_count}," \
               f"Load type: {load_type}, " \
               f"Total Execution Time: {self.total_execution_time} @ {self.rows_per_second:.2f} rows per second "

    class Batch:
        row_count = 0
        extract_started = None
        extract_completed = None
        load_completed = None
        status = Constants.ExecutionStatus.NOT_STARTED

        extract_execution_time = None
        extract_rows_per_second = 0
        load_execution_time = None
        load_rows_per_second = 0
        total_rows_per_second = 0
        total_execution_time = None

        def __init__(self):
            self.extract_started = datetime.now()
            self.status = Constants.ExecutionStatus.NOT_STARTED

        def extract_completed_successfully(self, row_count):
            self.status = Constants.ExecutionStatus.EXTRACT_COMPLETED_SUCCESSFULLY
            self.row_count = row_count
            self.extract_completed = datetime.now()
            self.extract_execution_time = self.extract_completed - self.extract_started
            if self.extract_execution_time.total_seconds() == 0:
                self.extract_rows_per_second = self.row_count
            else:
                self.extract_rows_per_second = self.row_count / self.extract_execution_time.total_seconds()

        def load_completed_successfully(self):
            self.status = Constants.ExecutionStatus.LOAD_COMPLETED_SUCCESSFULLY
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
            self.status = Constants.ExecutionStatus.SKIPPED_AS_ZERO_ROWS
            self.load_completed = datetime.now()

        def get_statistics(self):
            return f"Rows: {self.row_count}; " \
                   f"Extract Execution Time: {self.extract_execution_time} " \
                   f"@ {self.extract_rows_per_second:.2f} rows per second; " \
                   f"Load Execution Time: {self.load_execution_time} " \
                   f"@ {self.load_rows_per_second:.2f} rows per second; " \
                   f"Total Execution Time: {self.total_execution_time} " \
                   f"@ {self.total_rows_per_second:.2f} rows per second."
