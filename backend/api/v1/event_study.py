import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import get_db
from models import EventStudyResult
from schemas.event_study_schema import EventStudyResponse, CARPoint

router = APIRouter(prefix="/api/v1/event-study", tags=["event-study"])


@router.get("/{filing_id}", response_model=EventStudyResponse)
def get_event_study(filing_id: int, db: Session = Depends(get_db)):
    es = db.query(EventStudyResult).filter(EventStudyResult.filing_id == filing_id).first()
    if not es:
        raise HTTPException(status_code=404, detail="Event study not found for this filing")

    # Parse CAR timeseries
    car_ts = []
    if es.car_timeseries:
        try:
            raw = json.loads(es.car_timeseries)
            car_ts = [CARPoint(day=p["day"], car=p["car"]) for p in raw]
        except (json.JSONDecodeError, KeyError):
            pass

    return EventStudyResponse(
        filing_id=es.filing_id,
        ticker=es.ticker,
        benchmark_ticker=es.benchmark_ticker,
        car_pre30=es.car_pre30,
        car_post30=es.car_post30,
        car_post60=es.car_post60,
        car_post90=es.car_post90,
        car_timeseries=car_ts,
        alpha=es.alpha_daily,
        beta=es.beta,
        t_stat=es.t_stat_post30,
        p_value=es.p_value_post30,
    )
