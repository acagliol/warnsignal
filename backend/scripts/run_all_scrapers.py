"""
Master pipeline: run all WARN Act state scrapers in sequence.

Usage:
    python scripts/run_all_scrapers.py                  # run all 20 states
    python scripts/run_all_scrapers.py --states CA TX NY # run only selected states
    python scripts/run_all_scrapers.py --dry-run         # scrape but skip DB insert
"""

import sys
import os
import json
import time
import argparse
import logging
import traceback
from datetime import datetime

# ---------------------------------------------------------------------------
# Path setup -- allow running from repo root: python scripts/run_all_scrapers.py
# ---------------------------------------------------------------------------
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from database import engine, SessionLocal, Base
from models import WarnFiling

# ── Scraper registry ──────────────────────────────────────────────────────────
# Import every state scraper.  WA and MI exist on disk but are not yet
# re-exported from the scrapers package __init__.py, so we import them
# directly here to keep the pipeline self-contained.
from services.scrapers.ca_scraper import CAScraper
from services.scrapers.tx_scraper import TXScraper
from services.scrapers.ny_scraper import NYScraper
from services.scrapers.fl_scraper import FLScraper
from services.scrapers.il_scraper import ILScraper
from services.scrapers.nj_scraper import NJScraper
from services.scrapers.va_scraper import VAScraper
from services.scrapers.md_scraper import MDScraper
from services.scrapers.in_scraper import INScraper
from services.scrapers.oh_scraper import OHScraper
from services.scrapers.mo_scraper import MOScraper
from services.scrapers.ct_scraper import CTScraper
from services.scrapers.or_scraper import ORScraper
from services.scrapers.pa_scraper import PAScraper
from services.scrapers.nc_scraper import NCScraper
from services.scrapers.az_scraper import AZScraper
from services.scrapers.co_scraper import COScraper
from services.scrapers.ga_scraper import GAScraper
from services.scrapers.wa_scraper import WAScraper
from services.scrapers.mi_scraper import MIScraper

SCRAPER_REGISTRY = {
    "CA": CAScraper,
    "TX": TXScraper,
    "NY": NYScraper,
    "FL": FLScraper,
    "IL": ILScraper,
    "NJ": NJScraper,
    "VA": VAScraper,
    "MD": MDScraper,
    "IN": INScraper,
    "OH": OHScraper,
    "MO": MOScraper,
    "CT": CTScraper,
    "OR": ORScraper,
    "PA": PAScraper,
    "NC": NCScraper,
    "AZ": AZScraper,
    "CO": COScraper,
    "GA": GAScraper,
    "WA": WAScraper,
    "MI": MIScraper,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("run_all_scrapers")


# ── Per-state result container ────────────────────────────────────────────────
class StateResult:
    __slots__ = ("state", "scraped", "inserted", "skipped", "error", "elapsed")

    def __init__(self, state: str):
        self.state = state
        self.scraped = 0
        self.inserted = 0
        self.skipped = 0
        self.error: str | None = None
        self.elapsed = 0.0


# ── Core logic ────────────────────────────────────────────────────────────────
def insert_filings(db, state: str, filings: list[dict], dry_run: bool = False) -> tuple[int, int]:
    """Insert filings into the database, deduplicating against existing rows.

    Returns (inserted_count, skipped_count).
    """
    inserted = 0
    skipped = 0

    for filing_data in filings:
        company = (filing_data.get("company_name") or "").strip()
        filing_date = filing_data.get("filing_date")

        if not company or not filing_date:
            skipped += 1
            continue

        # Dedup: same state + company + filing_date + employees_affected
        existing = db.query(WarnFiling.id).filter(
            WarnFiling.state == state,
            WarnFiling.company_name_raw == company,
            WarnFiling.filing_date == filing_date,
            WarnFiling.employees_affected == filing_data.get("employees_affected"),
        ).first()

        if existing:
            skipped += 1
            continue

        if dry_run:
            inserted += 1
            continue

        record = WarnFiling(
            state=state,
            company_name_raw=company,
            filing_date=filing_date,
            layoff_date=filing_data.get("layoff_date"),
            employees_affected=filing_data.get("employees_affected"),
            location=filing_data.get("location"),
            source_url=filing_data.get("source_url"),
            raw_data=json.dumps(filing_data, default=str),
        )
        db.add(record)
        inserted += 1

    if not dry_run:
        db.commit()

    return inserted, skipped


def run_scraper(state: str, scraper_cls, db, dry_run: bool) -> StateResult:
    """Run a single state scraper and persist results. Never raises."""
    result = StateResult(state)
    t0 = time.perf_counter()

    try:
        scraper = scraper_cls()
        filings = scraper.scrape()
        result.scraped = len(filings)

        inserted, skipped = insert_filings(db, state, filings, dry_run=dry_run)
        result.inserted = inserted
        result.skipped = skipped

    except Exception:
        result.error = traceback.format_exc()
        logger.error(f"{state} scraper failed:\n{result.error}")
        try:
            db.rollback()
        except Exception:
            pass

    result.elapsed = time.perf_counter() - t0
    return result


# ── Summary table ─────────────────────────────────────────────────────────────
def print_summary(results: list[StateResult], wall_seconds: float):
    col_state = 7
    col_scraped = 10
    col_new = 12
    col_skip = 10
    col_time = 10
    col_status = 40

    header = (
        f"{'State':<{col_state}}"
        f"{'Scraped':>{col_scraped}}"
        f"{'New Insert':>{col_new}}"
        f"{'Skipped':>{col_skip}}"
        f"{'Time (s)':>{col_time}}"
        f"  {'Status':<{col_status}}"
    )
    sep = "-" * len(header)

    print()
    print(sep)
    print("SCRAPER PIPELINE SUMMARY")
    print(sep)
    print(header)
    print(sep)

    total_scraped = 0
    total_inserted = 0
    total_skipped = 0
    failed_states = []

    for r in results:
        total_scraped += r.scraped
        total_inserted += r.inserted
        total_skipped += r.skipped

        if r.error:
            # Show first line of the traceback only
            first_line = r.error.strip().splitlines()[-1][:col_status]
            status = f"FAILED: {first_line}"
            failed_states.append(r.state)
        else:
            status = "OK"

        print(
            f"{r.state:<{col_state}}"
            f"{r.scraped:>{col_scraped}}"
            f"{r.inserted:>{col_new}}"
            f"{r.skipped:>{col_skip}}"
            f"{r.elapsed:>{col_time}.1f}"
            f"  {status:<{col_status}}"
        )

    print(sep)
    print(
        f"{'TOTAL':<{col_state}}"
        f"{total_scraped:>{col_scraped}}"
        f"{total_inserted:>{col_new}}"
        f"{total_skipped:>{col_skip}}"
        f"{wall_seconds:>{col_time}.1f}"
        f"  {len(results) - len(failed_states)}/{len(results)} succeeded"
    )
    print(sep)

    if failed_states:
        print(f"\nFailed states: {', '.join(failed_states)}")
    print()


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Run WARN Act scrapers for all (or selected) states.",
    )
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="ST",
        help=(
            "Two-letter state codes to scrape (e.g. --states CA TX NY). "
            "Defaults to all 20 states."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape data but do not write to the database.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    # Determine which states to run
    if args.states:
        requested = [s.upper() for s in args.states]
        unknown = [s for s in requested if s not in SCRAPER_REGISTRY]
        if unknown:
            print(f"ERROR: Unknown state code(s): {', '.join(unknown)}")
            print(f"Available: {', '.join(sorted(SCRAPER_REGISTRY))}")
            sys.exit(1)
        states_to_run = requested
    else:
        states_to_run = list(SCRAPER_REGISTRY)

    # Ensure tables exist
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    mode = "DRY-RUN" if args.dry_run else "LIVE"
    logger.info(
        f"Starting scraper pipeline ({mode}) for {len(states_to_run)} state(s): "
        f"{', '.join(states_to_run)}"
    )

    wall_start = time.perf_counter()
    results: list[StateResult] = []

    for state in states_to_run:
        scraper_cls = SCRAPER_REGISTRY[state]
        logger.info(f"--- {state} ---")
        result = run_scraper(state, scraper_cls, db, dry_run=args.dry_run)
        results.append(result)

        if not result.error:
            logger.info(
                f"{state}: scraped={result.scraped}  "
                f"inserted={result.inserted}  skipped={result.skipped}  "
                f"({result.elapsed:.1f}s)"
            )

    wall_elapsed = time.perf_counter() - wall_start
    db.close()

    print_summary(results, wall_elapsed)

    # Exit with non-zero status if any state failed
    failed = [r for r in results if r.error]
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
