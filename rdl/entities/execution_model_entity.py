
import uuid

from sqlalchemy import Column, DateTime, Integer, String, Boolean, BigInteger, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.inspection import inspect

from rdl.entities import Base
from rdl.entities import ExecutionEntity
from rdl.shared import Constants


class ExecutionModelEntity(Base):
    __tablename__ = 'execution_model'
    __table_args__ = {'schema': Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}
    execution_id = Column(UUID(as_uuid=True), ForeignKey(
        f"{Constants.DATA_PIPELINE_EXECUTION_SCHEMA_NAME}."
        f"{inspect(ExecutionEntity).tables[0].name}."
        f"{inspect(ExecutionEntity).primary_key[0].name}"), nullable=True, primary_key=True)
    model_name = Column(String(250), nullable=False, primary_key=True)
    status = Column(String(25), nullable=False)
    last_sync_version = Column(BigInteger, nullable=False)
    sync_version = Column(BigInteger, nullable=False)
    is_full_refresh = Column(Boolean, nullable=False)
    full_refresh_reason = Column(String(100), nullable=False)
    started_on = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    completed_on = Column(DateTime(timezone=True),  nullable=True)
    execution_time_ms = Column(Integer, nullable=False, default=0)
    rows_processed = Column(Integer, nullable=False, default=0)
    model_checksum = Column(String(100), nullable=False)
    failure_reason = Column(String(1000), nullable=True)

    def __str__(self):
        load_type = 'FULL' if self.is_full_refresh else f"INCREMENTAL from " \
                                                        f"version '{self.last_sync_version}' " \
                                                        f"to '{self.sync_version}'"

        execution_tims_s = max(self.execution_time_ms // 1000, 1)
        rows_per_second = self.rows_processed / execution_tims_s
        return f"Rows: {self.rows_processed}, " \
               f"Load type: {load_type}, " \
               f"Total Execution Time: {execution_tims_s}s @ {rows_per_second:.2f} rows per second "
