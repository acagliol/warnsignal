from pydantic import BaseModel
from datetime import date
from typing import Optional


class SignalResponse(BaseModel):
    id: int
    ticker: str
    signal_date: date
    employees_affected: Optional[int] = None
    employees_pct: Optional[float] = None
    filing_lead_days: Optional[int] = None
    repeat_filer: bool = False
    sector: Optional[str] = None
    market_cap_bucket: Optional[str] = None
    composite_score: float
    car_post30: Optional[float] = None

    model_config = {"from_attributes": True}


class SignalListResponse(BaseModel):
    signals: list[SignalResponse]
