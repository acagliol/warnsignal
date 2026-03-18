from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from database import get_db
from models import WarnFiling, EntityMatch
from schemas.filing_schema import FilingResponse, FilingListResponse

router = APIRouter(prefix="/api/v1/filings", tags=["filings"])


@router.get("", response_model=FilingListResponse)
def list_filings(
    state: Optional[str] = None,
    ticker: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    resolved_only: bool = False,
    limit: int = Query(100, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    query = db.query(WarnFiling).outerjoin(EntityMatch)

    if state:
        query = query.filter(WarnFiling.state == state.upper())
    if ticker:
        query = query.filter(EntityMatch.ticker == ticker.upper())
    if start_date:
        query = query.filter(WarnFiling.filing_date >= start_date)
    if end_date:
        query = query.filter(WarnFiling.filing_date <= end_date)
    if resolved_only:
        query = query.filter(EntityMatch.ticker.isnot(None))

    total = query.count()
    filings = query.order_by(WarnFiling.filing_date.desc()).offset(offset).limit(limit).all()

    results = []
    for f in filings:
        em = f.entity_match
        results.append(FilingResponse(
            id=f.id,
            state=f.state,
            company_name_raw=f.company_name_raw,
            filing_date=f.filing_date,
            layoff_date=f.layoff_date,
            employees_affected=f.employees_affected,
            location=f.location,
            source_url=f.source_url,
            ticker=em.ticker if em else None,
            match_score=em.match_score if em else None,
            sector=em.sector if em else None,
            created_at=f.created_at,
        ))

    return FilingListResponse(total=total, filings=results)


@router.get("/{filing_id}")
def get_filing(filing_id: int, db: Session = Depends(get_db)):
    filing = db.query(WarnFiling).filter(WarnFiling.id == filing_id).first()
    if not filing:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Filing not found")

    em = filing.entity_match
    return {
        "id": filing.id,
        "state": filing.state,
        "company_name_raw": filing.company_name_raw,
        "filing_date": filing.filing_date,
        "layoff_date": filing.layoff_date,
        "employees_affected": filing.employees_affected,
        "location": filing.location,
        "source_url": filing.source_url,
        "entity_match": {
            "ticker": em.ticker,
            "match_method": em.match_method,
            "match_score": em.match_score,
            "sector": em.sector,
            "market_cap_bucket": em.market_cap_bucket,
            "is_confirmed": em.is_confirmed,
        } if em else None,
    }
