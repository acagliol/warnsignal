from pydantic import BaseModel
from datetime import date
from typing import Optional


class BacktestConfig(BaseModel):
    hold_days: int = 30
    max_positions: int = 20
    min_score: float = 0.0
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class BacktestTradeResponse(BaseModel):
    ticker: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    return_pct: float
    hold_days: int

    model_config = {"from_attributes": True}


class EquityCurvePoint(BaseModel):
    date: date
    value: float


class BacktestResultResponse(BaseModel):
    run_id: int
    run_name: Optional[str] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    total_return: Optional[float] = None
    win_rate: Optional[float] = None
    n_trades: Optional[int] = None
    equity_curve: list[EquityCurvePoint] = []
    trades: list[BacktestTradeResponse] = []


class BacktestStatsResponse(BaseModel):
    mean_car_post30: Optional[float] = None
    median_car_post30: Optional[float] = None
    t_stat: Optional[float] = None
    p_value: Optional[float] = None
    pct_negative: Optional[float] = None
    n_events: int = 0
    sector_breakdown: dict = {}
    cap_breakdown: dict = {}
    alpha_decay: list[dict] = []
