from sqlalchemy import Column, Integer, String, Date, DateTime, Text, Index
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base


class WarnFiling(Base):
    __tablename__ = "warn_filings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    state = Column(String(2), nullable=False, index=True)
    company_name_raw = Column(String(500), nullable=False)
    filing_date = Column(Date, nullable=False, index=True)
    layoff_date = Column(Date, nullable=True)
    employees_affected = Column(Integer, nullable=True)
    location = Column(String(500), nullable=True)
    source_url = Column(String(1000), nullable=True)
    raw_data = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    entity_match = relationship("EntityMatch", back_populates="filing", uselist=False)
    event_study = relationship("EventStudyResult", back_populates="filing", uselist=False)
    signal = relationship("Signal", back_populates="filing", uselist=False)

    __table_args__ = (
        Index("ix_filing_state_date", "state", "filing_date"),
    )

    def __repr__(self):
        return f"<WarnFiling {self.state} {self.company_name_raw} {self.filing_date}>"
