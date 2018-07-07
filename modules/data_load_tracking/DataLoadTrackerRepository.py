import logging
from modules.data_load_tracking.DataLoadExecution import DataLoadExecution, Base


class DataLoadTrackerRepository(object):
    def __init__(self, session_maker, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.session_maker = session_maker

    def create_tables(self, engine):
        engine.execute("CREATE SCHEMA IF NOT EXISTS {0}".format("data_pipeline"))
        Base.metadata.create_all(engine)

    def get_last_sync_version(self, model_name):
        session = self.session_maker()
        result = session.query(DataLoadExecution).filter_by(model_name=model_name, status="Load Completed Successfully").order_by(DataLoadExecution.completed_on).first()

        if result is None:
            return 0
        return result.next_sync_version


    def save(self, data_load_tracker):

        data_load_execution = DataLoadExecution(model_name=data_load_tracker.model_name,
                                                correlation_id=data_load_tracker.correlation_id,
                                                is_full_refresh=data_load_tracker.is_full_refresh,
                                                this_sync_version=data_load_tracker.change_tracking_info.this_sync_version,
                                                next_sync_version=data_load_tracker.change_tracking_info.next_sync_version,
                                                execution_time_ms=int(data_load_tracker.total_execution_time.total_seconds() * 1000),
                                                rows_processed=data_load_tracker.total_row_count,
                                                status=data_load_tracker.status)



        session = self.session_maker()
        session.add(data_load_execution)
        session.commit()
