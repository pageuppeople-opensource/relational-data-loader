import logging

from rdl.entities import ExecutionModelEntity, ExecutionEntity
from rdl.shared import Constants

from sqlalchemy import desc
from sqlalchemy import func


class DataLoadTrackerRepository(object):
    def __init__(self, session_maker, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.session_maker = session_maker

    def get_last_successful_data_load_execution(self, model_name):
        session = self.session_maker()
        result = (
            session.query(ExecutionModelEntity)
            .filter_by(
                model_name=model_name, status=Constants.ExecutionModelStatus.SUCCESSFUL
            )
            .order_by(desc(ExecutionModelEntity.completed_on))
            .first()
        )
        session.close()
        return result

    def create_execution(self):
        session = self.session_maker()
        new_execution = ExecutionEntity()
        session.add(new_execution)
        session.commit()
        self.logger.info(new_execution)
        execution_id = new_execution.execution_id
        session.close()
        return execution_id

    def fail_execution(self, execution_id, models_so_far):
        self.complete_execution(
            execution_id, models_so_far, status=Constants.ExecutionStatus.FAILED
        )

    def complete_execution(
        self,
        execution_id,
        total_number_of_models,
        status=Constants.ExecutionStatus.SUCCESSFUL,
    ):
        session = self.session_maker()
        current_execution = (
            session.query(ExecutionEntity)
            .filter(ExecutionEntity.execution_id == execution_id)
            .one()
        )

        execution_end_time = session.query(func.current_timestamp()).scalar()
        total_execution_seconds = max(
            (execution_end_time - current_execution.started_on).total_seconds(), 1
        )
        total_rows_processed = self.get_execution_rows(current_execution.execution_id)
        total_batches_processed = self.get_execution_batches(
            current_execution.execution_id
        )

        current_execution.models_processed = total_number_of_models
        current_execution.status = status
        current_execution.completed_on = execution_end_time
        current_execution.execution_time_s = total_execution_seconds
        current_execution.rows_processed = total_rows_processed
        current_execution.batches_processed = total_batches_processed
        session.commit()
        self.logger.info(f"Completed {current_execution}")
        session.close()
        return total_rows_processed

    def create_execution_model(self, data_load_tracker):
        new_execution_model = ExecutionModelEntity(
            execution_id=data_load_tracker.execution_id,
            model_name=data_load_tracker.model_name,
            status=data_load_tracker.status,
            last_sync_version=data_load_tracker.change_tracking_info.last_sync_version,
            sync_version=data_load_tracker.change_tracking_info.sync_version,
            is_full_refresh=data_load_tracker.is_full_refresh,
            full_refresh_reason=data_load_tracker.full_refresh_reason,
            model_checksum=data_load_tracker.model_checksum,
            failure_reason=data_load_tracker.failure_reason,
        )

        session = self.session_maker()
        session.add(new_execution_model)
        session.commit()
        session.close()

    def save_execution_model(self, data_load_tracker):
        session = self.session_maker()
        current_execution_model = (
            session.query(ExecutionModelEntity)
            .filter(ExecutionModelEntity.execution_id == data_load_tracker.execution_id)
            .filter(ExecutionModelEntity.model_name == data_load_tracker.model_name)
            .one()
        )

        execution_end_time = session.query(func.current_timestamp()).scalar()
        total_execution_seconds = max(
            (execution_end_time - current_execution_model.started_on).total_seconds(), 1
        )

        current_execution_model.completed_on = execution_end_time
        current_execution_model.execution_time_ms = int(total_execution_seconds * 1000)

        current_execution_model.batches_processed = len(data_load_tracker.batches)
        current_execution_model.rows_processed = data_load_tracker.total_row_count
        current_execution_model.status = data_load_tracker.status
        current_execution_model.is_full_refresh = data_load_tracker.is_full_refresh
        current_execution_model.full_refresh_reason = (
            data_load_tracker.full_refresh_reason
        )
        current_execution_model.model_checksum = data_load_tracker.model_checksum
        current_execution_model.failure_reason = data_load_tracker.failure_reason

        session.commit()
        self.logger.info(current_execution_model)
        session.close()

    def get_execution_rows(self, execution_id):
        session = self.session_maker()
        results = (
            session.query(func.sum(ExecutionModelEntity.rows_processed))
            .filter(ExecutionModelEntity.execution_id == execution_id)
            .scalar()
        )
        session.close()
        return results

    def get_execution_batches(self, execution_id):
        session = self.session_maker()
        results = (
            session.query(func.sum(ExecutionModelEntity.batches_processed))
            .filter(ExecutionModelEntity.execution_id == execution_id)
            .scalar()
        )
        session.close()
        return results

    def get_full_refresh_since(self, timestamp):
        session = self.session_maker()
        results = (
            session.query(ExecutionModelEntity.model_name)
            .filter(
                ExecutionModelEntity.completed_on > timestamp,
                ExecutionModelEntity.is_full_refresh,
            )
            .distinct(ExecutionModelEntity.model_name)
            .group_by(ExecutionModelEntity.model_name)
            .all()
        )
        session.close()
        return [r for (r,) in results]

    def get_incremental_since(self, timestamp):
        session = self.session_maker()
        results = (
            session.query(ExecutionModelEntity.model_name)
            .filter(
                ExecutionModelEntity.completed_on > timestamp,
                ExecutionModelEntity.is_full_refresh == False,
                ExecutionModelEntity.rows_processed > 0,
            )
            .distinct(ExecutionModelEntity.model_name)
            .group_by(ExecutionModelEntity.model_name)
            .all()
        )
        session.close()
        return [r for (r,) in results]

    def get_only_incremental_since(self, timestamp):
        session = self.session_maker()
        results = (
            session.query(ExecutionModelEntity.model_name)
            .filter(
                ExecutionModelEntity.completed_on > timestamp,
                ExecutionModelEntity.rows_processed > 0,
            )
            .distinct(ExecutionModelEntity.model_name)
            .group_by(ExecutionModelEntity.model_name)
            .having(func.bool_and(ExecutionModelEntity.is_full_refresh == False))
            .all()
        )
        session.close()
        return [r for (r,) in results]
