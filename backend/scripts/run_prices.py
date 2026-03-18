"""CLI: Fetch historical price data for all resolved tickers."""

import sys
import os
import logging
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database import engine, SessionLocal, Base
from models import WarnFiling, EntityMatch, PriceData
from services.market_data.price_loader import fetch_prices_batch, get_benchmark_ticker
from services.entity_resolution.sp1500 import SECTOR_ETF
from sqlalchemy import func

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    # Get all unique tickers with their date ranges
    results = (
        db.query(
            EntityMatch.ticker,
            EntityMatch.sector,
            func.min(WarnFiling.filing_date).label("earliest"),
            func.max(WarnFiling.filing_date).label("latest"),
        )
        .join(WarnFiling, EntityMatch.filing_id == WarnFiling.id)
        .filter(EntityMatch.ticker.isnot(None))
        .group_by(EntityMatch.ticker, EntityMatch.sector)
        .all()
    )

    # Collect all tickers we need prices for
    ticker_dates = {}
    for r in results:
        ticker = r.ticker
        # Need data from 300 days before earliest filing to 100 days after latest
        start = r.earliest - timedelta(days=400)
        end = r.latest + timedelta(days=150)

        if ticker in ticker_dates:
            ticker_dates[ticker] = (
                min(ticker_dates[ticker][0], start),
                max(ticker_dates[ticker][1], end),
            )
        else:
            ticker_dates[ticker] = (start, end)

        # Also add benchmark ETF
        benchmark = get_benchmark_ticker(r.sector)
        if benchmark not in ticker_dates:
            ticker_dates[benchmark] = (start, end)
        else:
            ticker_dates[benchmark] = (
                min(ticker_dates[benchmark][0], start),
                max(ticker_dates[benchmark][1], end),
            )

    # Add SPY as universal benchmark
    if ticker_dates:
        min_date = min(d[0] for d in ticker_dates.values())
        max_date = max(d[1] for d in ticker_dates.values())
        ticker_dates["SPY"] = (min_date, max_date)

    logger.info(f"Fetching prices for {len(ticker_dates)} tickers")

    # Fetch in batches
    all_tickers = list(ticker_dates.keys())
    batch_size = 50
    total_rows = 0

    for i in range(0, len(all_tickers), batch_size):
        batch = all_tickers[i:i + batch_size]
        # Use the widest date range for the batch
        batch_start = min(ticker_dates[t][0] for t in batch)
        batch_end = max(ticker_dates[t][1] for t in batch)

        logger.info(f"Fetching batch {i // batch_size + 1}: {len(batch)} tickers")
        prices = fetch_prices_batch(batch, batch_start, batch_end)

        for ticker, df in prices.items():
            for _, row in df.iterrows():
                # Check if already exists
                existing = db.query(PriceData).filter(
                    PriceData.ticker == ticker,
                    PriceData.date == row["date"],
                ).first()

                if existing:
                    continue

                price = PriceData(
                    ticker=ticker,
                    date=row["date"],
                    open=row.get("open"),
                    high=row.get("high"),
                    low=row.get("low"),
                    close=row["close"],
                    volume=int(row["volume"]) if row.get("volume") else None,
                )
                db.add(price)
                total_rows += 1

            db.commit()

    db.close()
    logger.info(f"Price loading complete. {total_rows} new price rows added")


if __name__ == "__main__":
    run()
