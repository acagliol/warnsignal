from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from database import get_db
from models import Signal, EventStudyResult
from schemas.signal_schema import SignalResponse, SignalListResponse

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


@router.get("", response_model=SignalListResponse)
def list_signals(
    sector: Optional[str] = None,
    min_score: float = 0.0,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(50, le=200),
    db: Session = Depends(get_db),
):
    query = db.query(Signal).outerjoin(EventStudyResult, Signal.filing_id == EventStudyResult.filing_id)

    if sector:
        query = query.filter(Signal.sector == sector)
    if min_score > 0:
        query = query.filter(Signal.composite_score >= min_score)
    if start_date:
        query = query.filter(Signal.signal_date >= start_date)
    if end_date:
        query = query.filter(Signal.signal_date <= end_date)

    signals = query.order_by(Signal.composite_score.desc()).limit(limit).all()

    results = []
    for s in signals:
        # Get CAR from event study if available
        es = db.query(EventStudyResult).filter(EventStudyResult.filing_id == s.filing_id).first()

        results.append(SignalResponse(
            id=s.id,
            ticker=s.ticker,
            signal_date=s.signal_date,
            employees_affected=s.employees_affected,
            employees_pct=s.employees_pct,
            filing_lead_days=s.filing_lead_days,
            repeat_filer=s.repeat_filer,
            sector=s.sector,
            market_cap_bucket=s.market_cap_bucket,
            composite_score=s.composite_score,
            car_post30=es.car_post30 if es else None,
        ))

    return SignalListResponse(signals=results)
