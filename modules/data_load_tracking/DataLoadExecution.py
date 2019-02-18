from sqlalchemy import Column, DateTime, Integer, String, Boolean, BigInteger
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()


class DataLoadExecution(Base):
    __tablename__ = 'data_load_execution'
    __table_args__ = {'schema': 'data_pipeline'}
    id = Column(Integer, primary_key=True)
    correlation_id = Column(UUID(as_uuid=True), nullable=True)
    model_name = Column(String(250), nullable=False)
    status = Column(String(25), nullable=False)
    this_sync_version = Column(BigInteger, nullable=False)
    next_sync_version = Column(BigInteger, nullable=False)
    is_full_refresh = Column(Boolean, nullable=False)
    full_refresh_reason = Column(String(100), nullable=False)
    completed_on = Column(DateTime(timezone=True), server_default=func.now())
    execution_time_ms = Column(Integer, nullable=False)
    rows_processed = Column(Integer, nullable=False)
    model_checksum = Column(String(100), nullable=False)
