import uuid

from sqlalchemy import Column, DateTime, Integer, String, BigInteger
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID

from rdl.entities.base import Base
from rdl.shared import Constants


class ExecutionEntity(Base):
    __tablename__ = 'execution'
    __table_args__ = {'schema': f'{Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}'}
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    execution_started = Column(DateTime(timezone=True), server_default=func.now())
    execution_ended = Column(DateTime(timezone=True))
    execution_time_s = Column(Integer, nullable=True)
    total_rows_processed = Column(BigInteger, nullable=True)
    total_models_processed = Column(Integer, nullable=True)
    status = Column(String(25), nullable=False, default=Constants.ExecutionStatus.STARTED)

    def __str__(self):
        if self.status == Constants.ExecutionStatus.STARTED:
            return f"Started Execution ID: {self.id} at {self.execution_started}"

        total_execution_seconds = self.execution_time_s
        execution_hours = total_execution_seconds // 3600
        execution_minutes = (total_execution_seconds // 60) % 60
        execution_seconds = total_execution_seconds % 60

        return f"Completed Execution ID: {self.id}" \
            f"; Models Processed: {self.total_models_processed:,}" \
            f"; Rows Processed: {self.total_rows_processed:,}" \
            f"; Execution Time: {execution_hours}h {execution_minutes}m {execution_seconds}s" \
            f"; Average rows processed per second: {(self.total_rows_processed//max(total_execution_seconds, 1)):,}."
