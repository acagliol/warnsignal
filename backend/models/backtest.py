from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base


class BacktestRun(Base):
    __tablename__ = "backtest_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_name = Column(String(200), nullable=True)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    config = Column(Text, nullable=True)  # JSON
    sharpe_ratio = Column(Float, nullable=True)
    max_drawdown = Column(Float, nullable=True)
    total_return = Column(Float, nullable=True)
    win_rate = Column(Float, nullable=True)
    n_trades = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    trades = relationship("BacktestTrade", back_populates="run")

    def __repr__(self):
        return f"<BacktestRun {self.run_name} sharpe={self.sharpe_ratio}>"


class BacktestTrade(Base):
    __tablename__ = "backtest_trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("backtest_runs.id"), nullable=False)
    filing_id = Column(Integer, ForeignKey("warn_filings.id"), nullable=True)
    ticker = Column(String(10), nullable=False)
    entry_date = Column(Date, nullable=False)
    exit_date = Column(Date, nullable=False)
    entry_price = Column(Float, nullable=False)
    exit_price = Column(Float, nullable=False)
    return_pct = Column(Float, nullable=False)
    hold_days = Column(Integer, nullable=False)
    created_at = Column(DateTime, server_default=func.now())

    run = relationship("BacktestRun", back_populates="trades")

    def __repr__(self):
        return f"<BacktestTrade {self.ticker} {self.entry_date} ret={self.return_pct:.2%}>"
