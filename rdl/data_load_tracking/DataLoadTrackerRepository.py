import logging

from rdl.data_load_tracking.DataLoadExecution import DataLoadExecution, Base
from rdl.shared import Constants

from sqlalchemy import desc
from sqlalchemy import func


class DataLoadTrackerRepository(object):
    def __init__(self, session_maker, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.session_maker = session_maker

    def ensure_schema_exists(self, engine):
        engine.execute(f"CREATE SCHEMA IF NOT EXISTS {Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}")
        Base.metadata.create_all(engine)

    def get_last_successful_data_load_execution(self, model_name):
        session = self.session_maker()
        result = session.query(DataLoadExecution)\
            .filter_by(model_name=model_name, status=Constants.ExecutionStatus.COMPLETED_SUCCESSFULLY)\
            .order_by(desc(DataLoadExecution.completed_on))\
            .first()
        session.close()
        return result

    def save(self, data_load_tracker):
        data_load_execution = DataLoadExecution(
            correlation_id=data_load_tracker.correlation_id,
            model_name=data_load_tracker.model_name,
            status=data_load_tracker.status,
            last_sync_version=data_load_tracker.change_tracking_info.last_sync_version,
            sync_version=data_load_tracker.change_tracking_info.sync_version,
            is_full_refresh=data_load_tracker.is_full_refresh,
            full_refresh_reason=data_load_tracker.full_refresh_reason,
            execution_time_ms=int(data_load_tracker.total_execution_time.total_seconds() * 1000),
            rows_processed=data_load_tracker.total_row_count,
            model_checksum=data_load_tracker.model_checksum)

        session = self.session_maker()
        session.add(data_load_execution)
        session.commit()
        session.close()

    def get_full_refresh_since(self, timestamp):
        session = self.session_maker()
        results = session.query(DataLoadExecution.model_name)\
            .filter(DataLoadExecution.completed_on > timestamp,
                    DataLoadExecution.is_full_refresh)\
            .distinct(DataLoadExecution.model_name)\
            .group_by(DataLoadExecution.model_name)\
            .all()
        session.close()
        return [r for (r, ) in results]

    def get_incremental_since(self, timestamp):
        session = self.session_maker()
        results = session.query(DataLoadExecution.model_name)\
            .filter(DataLoadExecution.completed_on > timestamp,
                    DataLoadExecution.is_full_refresh == False,
                    DataLoadExecution.rows_processed > 0)\
            .distinct(DataLoadExecution.model_name)\
            .group_by(DataLoadExecution.model_name)\
            .all()
        session.close()
        return [r for (r, ) in results]

    def get_only_incremental_since(self, timestamp):
        session = self.session_maker()
        results = session.query(DataLoadExecution.model_name)\
            .filter(DataLoadExecution.completed_on > timestamp,
                    DataLoadExecution.rows_processed > 0)\
            .distinct(DataLoadExecution.model_name)\
            .group_by(DataLoadExecution.model_name)\
            .having(func.bool_and(DataLoadExecution.is_full_refresh == False))\
            .all()
        session.close()
        return [r for (r, ) in results]
