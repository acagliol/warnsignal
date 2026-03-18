"""CLI: Run entity resolution on all unresolved WARN filings."""

import sys
import os
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database import engine, SessionLocal, Base
from models import WarnFiling, EntityMatch
from services.entity_resolution.resolver import EntityResolver
from services.market_data.price_loader import get_company_info
from services.entity_resolution.sp1500 import get_market_cap_bucket
from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    resolver = EntityResolver(
        match_threshold=settings.MATCH_THRESHOLD,
        sec_user_agent=settings.SEC_USER_AGENT,
    )

    # Get unresolved filings
    resolved_ids = db.query(EntityMatch.filing_id).subquery()
    unresolved = db.query(WarnFiling).filter(~WarnFiling.id.in_(resolved_ids)).all()

    logger.info(f"Found {len(unresolved)} unresolved filings")

    resolved_count = 0
    unresolved_count = 0
    resolution_cache: dict = {}
    yfinance_cache: dict = {}
    BATCH_SIZE = 200

    for i, filing in enumerate(unresolved):
        raw_name = filing.company_name_raw

        if raw_name in resolution_cache:
            result = resolution_cache[raw_name]
        else:
            result = resolver.resolve(raw_name)

            if result["ticker"] and not result.get("sector"):
                ticker = result["ticker"]
                if ticker not in yfinance_cache:
                    info = get_company_info(ticker)
                    yfinance_cache[ticker] = info
                info = yfinance_cache[ticker]
                if info.get("sector"):
                    result["sector"] = info["sector"]
                if info.get("market_cap"):
                    result["market_cap_bucket"] = get_market_cap_bucket(info["market_cap"])

            resolution_cache[raw_name] = result

        match = EntityMatch(
            filing_id=filing.id,
            ticker=result["ticker"],
            company_name_matched=result["company_name_matched"],
            match_method=result["match_method"],
            match_score=result["match_score"],
            cik=result["cik"],
            sector=result["sector"],
            market_cap_bucket=result["market_cap_bucket"],
            is_confirmed=result["match_score"] >= 95 if result["match_score"] else False,
        )
        db.add(match)

        if result["ticker"]:
            resolved_count += 1
            logger.info(f"  {raw_name} -> {result['ticker']} ({result['match_method']}, score={result['match_score']:.0f})")
        else:
            unresolved_count += 1

        if (i + 1) % BATCH_SIZE == 0:
            db.commit()
            logger.info(f"Progress: {i+1}/{len(unresolved)} filings processed")

    db.commit()
    db.close()

    logger.info(f"Resolution complete. Resolved: {resolved_count}, Unresolved: {unresolved_count}")


if __name__ == "__main__":
    run()
