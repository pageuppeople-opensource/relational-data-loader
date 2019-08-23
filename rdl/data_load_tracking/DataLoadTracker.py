from datetime import datetime
from rdl.shared import Constants


class DataLoadTracker:
    def __init__(
        self,
        execution_id,
        model_name,
        model_checksum,
        model_config,
        is_full_refresh,
        full_refresh_reason,
        change_tracking_info,
    ):
        self.model_name = model_name
        self.model_checksum = model_checksum
        self.model_config = model_config
        self.is_full_refresh = is_full_refresh
        self.status = Constants.ExecutionModelStatus.STARTED
        self.change_tracking_info = change_tracking_info
        self.execution_id = execution_id
        self.full_refresh_reason = full_refresh_reason
        self.failure_reason = None
        self.batches = []
        self.total_row_count = 0

    def start_batch(self):
        batch = self.Batch()
        self.batches.append(batch)
        return batch

    def data_load_successful(self):
        self.data_load_completed(Constants.ExecutionModelStatus.SUCCESSFUL)

    def data_load_failed(self, failure_reason=None):
        self.data_load_completed(Constants.ExecutionModelStatus.FAILED, failure_reason)

    def data_load_completed(self, execution_status, failure_reason=None):
        self.status = execution_status
        self.failure_reason = failure_reason
        for batch in self.batches:
            self.total_row_count += batch.row_count

    class Batch:
        row_count = 0
        extract_started = None
        extract_completed = None
        load_completed = None
        status = Constants.BatchExecutionStatus.STARTED

        extract_execution_time = None
        extract_rows_per_second = 0
        load_execution_time = None
        load_rows_per_second = 0
        total_rows_per_second = 0
        total_execution_time = None

        def __init__(self):
            self.extract_started = datetime.now()
            self.status = Constants.BatchExecutionStatus.STARTED

        def extract_completed_successfully(self, row_count):
            self.status = Constants.BatchExecutionStatus.EXTRACT_COMPLETED_SUCCESSFULLY
            self.extract_completed = datetime.now()

            self.row_count = row_count

            self.extract_execution_time = self.extract_completed - self.extract_started
            if self.extract_execution_time.total_seconds() == 0:
                self.extract_rows_per_second = self.row_count
            else:
                self.extract_rows_per_second = (
                    self.row_count / self.extract_execution_time.total_seconds()
                )

        def load_completed_successfully(self):
            self.status = Constants.BatchExecutionStatus.LOAD_COMPLETED_SUCCESSFULLY
            self.load_completed = datetime.now()

            self.load_execution_time = self.load_completed - self.extract_completed
            if self.load_execution_time.total_seconds() == 0:
                self.load_rows_per_second = self.row_count
            else:
                self.load_rows_per_second = (
                    self.row_count / self.load_execution_time.total_seconds()
                )

            self.total_execution_time = self.load_completed - self.extract_started
            if self.total_execution_time.total_seconds() == 0:
                self.total_rows_per_second = self.row_count
            else:
                self.total_rows_per_second = (
                    self.row_count / self.total_execution_time.total_seconds()
                )

        def load_skipped_due_to_zero_rows(self):
            self.status = Constants.BatchExecutionStatus.SKIPPED_AS_ZERO_ROWS
            self.load_completed = datetime.now()

        def get_statistics(self):
            return (
                f"Rows: {self.row_count}; "
                f"Extract Execution Time: {self.extract_execution_time} "
                f"@ {self.extract_rows_per_second:.2f} rows per second; "
                f"Load Execution Time: {self.load_execution_time} "
                f"@ {self.load_rows_per_second:.2f} rows per second; "
                f"Total Execution Time: {self.total_execution_time} "
                f"@ {self.total_rows_per_second:.2f} rows per second."
            )
