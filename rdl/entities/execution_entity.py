import uuid

from sqlalchemy import Column, DateTime, Integer, String, BigInteger
from sqlalchemy.sql import func

from rdl.entities.base import Base
from rdl.shared import Constants


class ExecutionEntity(Base):
    __tablename__ = "execution"
    __table_args__ = {"schema": f"{Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}"}

    execution_id = Column(String(250), primary_key=True, default=f"{uuid.uuid4()}")
    created_on = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.timezone("UTC", func.getdate()),
    )
    updated_on = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.timezone("UTC", func.getdate()),
        onupdate=func.timezone("UTC", func.getdate()),
    )
    status = Column(
        String(50),
        nullable=False,
        server_default=str(Constants.ExecutionStatus.STARTED),
    )
    started_on = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.timezone("UTC", func.getdate()),
    )
    completed_on = Column(DateTime(timezone=True), nullable=True)
    execution_time_s = Column(BigInteger, nullable=True)
    rows_processed = Column(BigInteger, nullable=True)
    batches_processed = Column(Integer, nullable=True)
    models_processed = Column(Integer, nullable=True)

    def __str__(self):
        execution_time_str = None
        rows_per_second = None

        if self.execution_time_s:
            total_execution_seconds = self.execution_time_s

            execution_hours = total_execution_seconds // 3600
            execution_minutes = (total_execution_seconds // 60) % 60
            execution_seconds = total_execution_seconds % 60
            execution_time_str = (
                f"{execution_hours}h {execution_minutes}m {execution_seconds}s"
            )

            if self.rows_processed:
                rows_per_second = self.rows_processed // max(total_execution_seconds, 1)

        return (
            "Execution ID: {exec_id}; "
            "Status: {status}; "
            "Started on: {started}; "
            "Completed on: {completed}; "
            "Execution time: {exec_time}; "
            "Models processed: {models}; "
            "Batches processed: {batches};"
            "Rows processed: {rows}; "
            "Average rows processed per second: {rows_per_second};".format(
                exec_id=self.execution_id,
                status=self.status,
                started=self.started_on.isoformat(),
                completed=self.completed_on.isoformat() if self.completed_on else "n/a",
                exec_time=execution_time_str if execution_time_str else "n/a",
                models=f"{self.models_processed:,}" if self.models_processed else "n/a",
                batches=f"{self.batches_processed:,}"
                if self.batches_processed
                else "n/a",
                rows=f"{self.rows_processed:,}" if self.rows_processed else "n/a",
                rows_per_second=f"{rows_per_second:,.2f}" if rows_per_second else "n/a",
            )
        )
