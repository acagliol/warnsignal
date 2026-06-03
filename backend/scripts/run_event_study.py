"""CLI: Run event studies (CAR calculations) for all resolved filings."""

import sys
import os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
from database import engine, SessionLocal, Base
from models import WarnFiling, EntityMatch, PriceData, EventStudyResult
from services.event_study.car_calculator import run_event_study
from services.market_data.price_loader import get_benchmark_ticker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    # Get resolved filings without event study results
    existing_ids = db.query(EventStudyResult.filing_id).subquery()
    filings = (
        db.query(WarnFiling, EntityMatch)
        .join(EntityMatch, WarnFiling.id == EntityMatch.filing_id)
        .filter(EntityMatch.ticker.isnot(None))
        .filter(~WarnFiling.id.in_(existing_ids))
        .all()
    )

    logger.info(f"Running event studies for {len(filings)} filings")

    success = 0
    failed = 0

    for filing, entity in filings:
        ticker = entity.ticker
        benchmark = get_benchmark_ticker(entity.sector)

        # Load prices
        stock_prices = pd.DataFrame(
            [(p.date, p.close) for p in db.query(PriceData).filter(PriceData.ticker == ticker).all()],
            columns=["date", "close"],
        )
        bench_prices = pd.DataFrame(
            [(p.date, p.close) for p in db.query(PriceData).filter(PriceData.ticker == benchmark).all()],
            columns=["date", "close"],
        )

        if stock_prices.empty or bench_prices.empty:
            logger.warning(f"No price data for {ticker} or {benchmark}, skipping")
            failed += 1
            continue

        result = run_event_study(stock_prices, bench_prices, filing.filing_date)

        if result is None:
            failed += 1
            continue

        def _to_float(v):
            """Convert numpy types to native Python float for SQLAlchemy."""
            if v is None:
                return None
            if isinstance(v, (np.floating, np.integer)):
                return float(v)
            return v

        es = EventStudyResult(
            filing_id=filing.id,
            ticker=ticker,
            benchmark_ticker=benchmark,
            estimation_window_start=result.get("estimation_window_start"),
            estimation_window_end=result.get("estimation_window_end"),
            car_pre30=_to_float(result.get("car_pre30")),
            car_post30=_to_float(result.get("car_post30")),
            car_post60=_to_float(result.get("car_post60")),
            car_post90=_to_float(result.get("car_post90")),
            car_timeseries=result.get("car_timeseries"),
            alpha_daily=_to_float(result.get("alpha_daily")),
            beta=_to_float(result.get("beta")),
            t_stat_post30=_to_float(result.get("t_stat_post30")),
            p_value_post30=_to_float(result.get("p_value_post30")),
        )
        db.add(es)
        success += 1

        if success % 50 == 0:
            db.commit()
            logger.info(f"Progress: {success} completed, {failed} failed")

    db.commit()
    db.close()
    logger.info(f"Event study complete. Success: {success}, Failed: {failed}")


if __name__ == "__main__":
    run()
