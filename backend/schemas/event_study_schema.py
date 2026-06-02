from pydantic import BaseModel
from typing import Optional


class CARPoint(BaseModel):
    day: int
    car: float


class EventStudyResponse(BaseModel):
    filing_id: int
    ticker: str
    benchmark_ticker: str
    car_pre30: Optional[float] = None
    car_post30: Optional[float] = None
    car_post60: Optional[float] = None
    car_post90: Optional[float] = None
    car_timeseries: list[CARPoint] = []
    alpha: Optional[float] = None
    beta: Optional[float] = None
    t_stat: Optional[float] = None
    p_value: Optional[float] = None

    model_config = {"from_attributes": True}
