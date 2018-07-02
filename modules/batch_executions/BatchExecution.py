from sqlalchemy import Column, DateTime, Integer, String, Boolean, BigInteger
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class BatchExecution(Base):
    __tablename__ = 'batch_execution'
    id = Column(Integer, primary_key=True)
    model_name = Column(String(250), nullable=False)
    is_full_refresh = Column(Boolean, nullable=False)
    start_synchronization_version = Column(BigInteger, nullable=False)
    next_bookmark_synchronization_version = Column(BigInteger, nullable=False)
    started_on = Column(DateTime(timezone=True), server_default=func.now())