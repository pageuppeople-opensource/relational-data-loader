import uuid

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Boolean,
    BigInteger,
    ForeignKey,
    Index,
)
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.inspection import inspect

from rdl.entities import Base
from rdl.entities import ExecutionEntity
from rdl.shared import Constants


class ExecutionModelEntity(Base):
    __tablename__ = "execution_model"
    __table_args__ = {"schema": Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}
    execution_model_id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    created_on = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_on = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    execution_id = Column(
        UUID(as_uuid=True),
        ForeignKey(
            f"{Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}."
            f"{inspect(ExecutionEntity).tables[0].name}."
            f"{inspect(ExecutionEntity).primary_key[0].name}"
        ),
        nullable=False,
    )
    model_name = Column(String(250), nullable=False)
    status = Column(
        String(50),
        nullable=False,
        server_default=str(Constants.ExecutionModelStatus.STARTED),
        index=True
    )
    last_sync_version = Column(BigInteger, nullable=False)
    sync_version = Column(BigInteger, nullable=False)
    is_full_refresh = Column(Boolean, nullable=False)
    full_refresh_reason = Column(String(100), nullable=False)
    started_on = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    completed_on = Column(DateTime(timezone=True), nullable=True, index=True)
    execution_time_ms = Column(BigInteger, nullable=True)
    rows_processed = Column(BigInteger, nullable=True)
    batches_processed = Column(Integer, nullable=True)
    model_checksum = Column(String(100), nullable=False)
    failure_reason = Column(String(1000), nullable=True)

    index_on_execution_id_model_name = Index("execution_model__index_on_execution_id_model_name", execution_id, model_name, unique=True)
    index_on_model_name_completed_on = Index("execution_model__index_on_model_name_completed_on", model_name, completed_on)

    def __str__(self):
        load_type = (
            f"FULL ({self.full_refresh_reason})"
            if self.is_full_refresh
            else f"INCREMENTAL from version '{self.last_sync_version}' to '{self.sync_version}'"
        )
        execution_time_s = None
        rows_per_second = None

        if self.execution_time_ms:
            execution_time_s = max(self.execution_time_ms // 1000, 1)

            if self.rows_processed:
                rows_per_second = self.rows_processed / execution_time_s

        return (
            "Model: {model}; "
            "Load type: {load_type}; "
            "Status: {status}; "
            "Started on: {started}; "
            "Completed on: {completed}; "
            "Execution time: {exec_time}; "
            "Batches processed: {batches}; "
            "Rows processed: {rows}; "
            "Average rows processed per second: {rows_per_second};".format(
                model=self.model_name,
                load_type=load_type,
                status=self.status,
                started=self.started_on.isoformat(),
                completed=self.completed_on.isoformat() if self.completed_on else "n/a",
                exec_time=f"{execution_time_s}s" if execution_time_s else "n/a",
                batches=f"{self.batches_processed:,}"
                if self.batches_processed
                else "n/a",
                rows=f"{self.rows_processed:,}" if self.rows_processed else "n/a",
                rows_per_second=f"{rows_per_second:,.2f}" if rows_per_second else "n/a",
            )
        )
