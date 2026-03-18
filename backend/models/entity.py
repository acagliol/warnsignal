from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base


class EntityMatch(Base):
    __tablename__ = "entity_matches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filing_id = Column(Integer, ForeignKey("warn_filings.id"), unique=True, nullable=False)
    ticker = Column(String(10), nullable=True, index=True)
    company_name_matched = Column(String(500), nullable=True)
    match_method = Column(String(50), nullable=True)
    match_score = Column(Float, nullable=True)
    cik = Column(String(20), nullable=True)
    sector = Column(String(100), nullable=True)
    market_cap_bucket = Column(String(20), nullable=True)
    is_confirmed = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())

    filing = relationship("WarnFiling", back_populates="entity_match")

    def __repr__(self):
        return f"<EntityMatch filing={self.filing_id} ticker={self.ticker} score={self.match_score}>"
