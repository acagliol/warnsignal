from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional


class FilingBase(BaseModel):
    state: str
    company_name_raw: str
    filing_date: date
    layoff_date: Optional[date] = None
    employees_affected: Optional[int] = None
    location: Optional[str] = None


class FilingResponse(FilingBase):
    id: int
    source_url: Optional[str] = None
    ticker: Optional[str] = None
    match_score: Optional[float] = None
    sector: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class FilingListResponse(BaseModel):
    total: int
    filings: list[FilingResponse]
