"""CLI: Run all WARN Act scrapers and store results to database."""

import sys
import os
import json
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database import engine, SessionLocal, Base
from models import WarnFiling
from services.scrapers import ALL_SCRAPERS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    total_new = 0
    total_skipped = 0

    for ScraperClass in ALL_SCRAPERS:
        scraper = ScraperClass()
        logger.info(f"Running {scraper.STATE} scraper...")

        try:
            filings = scraper.scrape()
            logger.info(f"{scraper.STATE}: scraped {len(filings)} raw filings")

            for filing_data in filings:
                company = filing_data.get("company_name", "").strip()
                filing_date = filing_data.get("filing_date")

                if not company or not filing_date:
                    continue

                # Dedup check
                existing = db.query(WarnFiling).filter(
                    WarnFiling.state == scraper.STATE,
                    WarnFiling.company_name_raw == company,
                    WarnFiling.filing_date == filing_date,
                    WarnFiling.employees_affected == filing_data.get("employees_affected"),
                ).first()

                if existing:
                    total_skipped += 1
                    continue

                record = WarnFiling(
                    state=scraper.STATE,
                    company_name_raw=company,
                    filing_date=filing_date,
                    layoff_date=filing_data.get("layoff_date"),
                    employees_affected=filing_data.get("employees_affected"),
                    location=filing_data.get("location"),
                    source_url=filing_data.get("source_url"),
                    raw_data=json.dumps(filing_data, default=str),
                )
                db.add(record)
                total_new += 1

            db.commit()
            logger.info(f"{scraper.STATE}: committed {total_new} new filings")

        except Exception as e:
            logger.error(f"{scraper.STATE} scraper failed: {e}")
            db.rollback()

    db.close()
    logger.info(f"Scraping complete. New: {total_new}, Skipped (dupes): {total_skipped}")


if __name__ == "__main__":
    run()
