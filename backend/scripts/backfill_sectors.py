"""CLI: Backfill missing sector and market_cap_bucket data for matched entities."""

import sys
import os
import logging
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database import engine, SessionLocal, Base
from models import EntityMatch
from services.market_data.price_loader import get_company_info

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    # Find entities with ticker but missing sector
    missing = (
        db.query(EntityMatch)
        .filter(EntityMatch.ticker.isnot(None))
        .filter(
            (EntityMatch.sector.is_(None)) | (EntityMatch.market_cap_bucket.is_(None))
        )
        .all()
    )

    logger.info(f"Found {len(missing)} entities with missing sector/cap data")

    updated = 0
    failed = 0

    # Deduplicate by ticker to avoid redundant API calls
    tickers_seen = {}
    for entity in missing:
        ticker = entity.ticker
        if ticker in tickers_seen:
            info = tickers_seen[ticker]
        else:
            try:
                info = get_company_info(ticker)
                tickers_seen[ticker] = info
                time.sleep(0.5)  # Rate limit
            except Exception as e:
                logger.warning(f"Failed to get info for {ticker}: {e}")
                tickers_seen[ticker] = {}
                failed += 1
                continue

        sector = info.get("sector")
        market_cap = info.get("market_cap") or info.get("marketCap")
        employees = info.get("full_time_employees") or info.get("fullTimeEmployees")

        changed = False
        if sector and not entity.sector:
            entity.sector = sector
            changed = True

        if market_cap and not entity.market_cap_bucket:
            if market_cap >= 200e9:
                entity.market_cap_bucket = "mega"
            elif market_cap >= 10e9:
                entity.market_cap_bucket = "large"
            elif market_cap >= 2e9:
                entity.market_cap_bucket = "mid"
            elif market_cap >= 300e6:
                entity.market_cap_bucket = "small"
            else:
                entity.market_cap_bucket = "micro"
            changed = True

        if changed:
            updated += 1
            if updated % 20 == 0:
                db.commit()
                logger.info(f"Progress: {updated} updated, {failed} failed")

    db.commit()
    db.close()
    logger.info(f"Backfill complete. Updated: {updated}, Failed: {failed}")


if __name__ == "__main__":
    run()
