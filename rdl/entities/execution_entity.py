import uuid

from sqlalchemy import Column, DateTime, Integer, String, BigInteger
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID

from rdl.entities.base import Base
from rdl.shared import Constants


class ExecutionEntity(Base):
    __tablename__ = 'execution'
    __table_args__ = {'schema': f'{Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}'}
    execution_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_on = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_on = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    status = Column(String(50), nullable=False, server_default=str(Constants.ExecutionStatus.STARTED))
    started_on = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    completed_on = Column(DateTime(timezone=True), nullable=True)
    execution_time_s = Column(BigInteger, nullable=True)
    rows_processed = Column(BigInteger, nullable=True)
    batches_processed = Column(Integer, nullable=True)
    models_processed = Column(Integer, nullable=True)

    def __str__(self):
        if self.status == Constants.ExecutionStatus.STARTED:
            return f"Started Execution ID: {self.execution_id} at {self.started_on}"

        total_execution_seconds = self.execution_time_s
        execution_hours = total_execution_seconds // 3600
        execution_minutes = (total_execution_seconds // 60) % 60
        execution_seconds = total_execution_seconds % 60

        return f"Completed Execution ID: {self.execution_id}" \
            f"; Models Processed: {self.models_processed:,}" \
            f"; Rows Processed: {self.rows_processed:,}" \
            f"; Execution Time: {execution_hours}h {execution_minutes}m {execution_seconds}s" \
            f"; Average rows processed per second: {(self.rows_processed//max(total_execution_seconds, 1)):,}."
