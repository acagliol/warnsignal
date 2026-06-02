from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base


class EventStudyResult(Base):
    __tablename__ = "event_study_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filing_id = Column(Integer, ForeignKey("warn_filings.id"), unique=True, nullable=False)
    ticker = Column(String(10), nullable=False)
    benchmark_ticker = Column(String(10), nullable=False)
    estimation_window_start = Column(Date, nullable=True)
    estimation_window_end = Column(Date, nullable=True)
    car_pre30 = Column(Float, nullable=True)
    car_post30 = Column(Float, nullable=True)
    car_post60 = Column(Float, nullable=True)
    car_post90 = Column(Float, nullable=True)
    car_timeseries = Column(Text, nullable=True)  # JSON array of {day, car}
    alpha_daily = Column(Float, nullable=True)
    beta = Column(Float, nullable=True)
    t_stat_post30 = Column(Float, nullable=True)
    p_value_post30 = Column(Float, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    filing = relationship("WarnFiling", back_populates="event_study")

    def __repr__(self):
        return f"<EventStudyResult filing={self.filing_id} ticker={self.ticker} CAR30={self.car_post30}>"
