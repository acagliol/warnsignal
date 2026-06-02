from sqlalchemy import Column, Integer, String, Float, Boolean, Date, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base


class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filing_id = Column(Integer, ForeignKey("warn_filings.id"), unique=True, nullable=False)
    ticker = Column(String(10), nullable=False, index=True)
    signal_date = Column(Date, nullable=False, index=True)
    employees_affected = Column(Integer, nullable=True)
    employees_pct = Column(Float, nullable=True)
    filing_lead_days = Column(Integer, nullable=True)
    repeat_filer = Column(Boolean, default=False)
    sector = Column(String(100), nullable=True)
    market_cap_bucket = Column(String(20), nullable=True)
    composite_score = Column(Float, nullable=False)
    direction = Column(String(10), default="short")
    created_at = Column(DateTime, server_default=func.now())

    filing = relationship("WarnFiling", back_populates="signal")

    def __repr__(self):
        return f"<Signal {self.ticker} {self.signal_date} score={self.composite_score}>"
