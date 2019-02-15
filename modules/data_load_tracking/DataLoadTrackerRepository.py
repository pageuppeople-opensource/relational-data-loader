import logging
from sqlalchemy import desc
from modules.data_load_tracking.DataLoadExecution import DataLoadExecution, Base
from modules.Shared import Constants


class DataLoadTrackerRepository(object):
    def __init__(self, session_maker, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.session_maker = session_maker

    def ensure_schema_exists(self, engine):
        engine.execute(f"CREATE SCHEMA IF NOT EXISTS {Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}")
        Base.metadata.create_all(engine)

    def get_last_successful_data_load_execution(self, model_name):
        session = self.session_maker()
        return session.query(DataLoadExecution).filter_by(
            model_name=model_name,
            status="Completed Successfully").order_by(
            desc(
                DataLoadExecution.completed_on)).first()

    def save(self, data_load_tracker):

        data_load_execution = DataLoadExecution(
            model_name=data_load_tracker.model_name,
            correlation_id=data_load_tracker.correlation_id,
            is_full_refresh=data_load_tracker.is_full_refresh,
            this_sync_version=data_load_tracker.change_tracking_info.this_sync_version,
            next_sync_version=data_load_tracker.change_tracking_info.next_sync_version,
            execution_time_ms=int(
                data_load_tracker.total_execution_time.total_seconds() * 1000),
            rows_processed=data_load_tracker.total_row_count,
            status=data_load_tracker.status,
            model_checksum=data_load_tracker.model_checksum,
            full_refresh_reason=data_load_tracker.full_refresh_reason)

        session = self.session_maker()
        session.add(data_load_execution)
        session.commit()
