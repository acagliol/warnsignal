"""CLI: Validate the signal against known distress cases.

Tests BBBY (Bed Bath & Beyond), RAD (Rite Aid), PRTY (Party City), REV (Revlon)
to verify that WARN filings appeared before stock collapses.
"""

import sys
import os
import logging
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from services.event_study.car_calculator import run_event_study
from services.market_data.price_loader import fetch_prices

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Known WARN filing events for validation
# These are approximate dates based on public records
ANCHOR_EVENTS = {
    "BBBY": {
        "description": "Bed Bath & Beyond — multiple WARN filings before bankruptcy (2023)",
        "filing_date": date(2023, 1, 5),
        "benchmark": "XLY",
    },
    "RAD": {
        "description": "Rite Aid — WARN filings before Chapter 11 (2023)",
        "filing_date": date(2023, 6, 15),
        "benchmark": "XLP",
    },
    "PRTY": {
        "description": "Party City — WARN filings before bankruptcy (2023)",
        "filing_date": date(2022, 12, 1),
        "benchmark": "XLY",
    },
    "REV": {
        "description": "Revlon — WARN filings before Chapter 11 (2022)",
        "filing_date": date(2022, 5, 1),
        "benchmark": "XLP",
    },
}


def validate():
    print("\n" + "=" * 70)
    print("WARN SIGNAL VALIDATION: KNOWN DISTRESS EVENTS")
    print("=" * 70)

    results = {}

    for ticker, event in ANCHOR_EVENTS.items():
        print(f"\n--- {ticker}: {event['description']} ---")
        filing_date = event["filing_date"]
        benchmark = event["benchmark"]

        # Fetch prices (need ~300 days before to 100 days after)
        start = filing_date - timedelta(days=400)
        end = filing_date + timedelta(days=150)

        stock_df = fetch_prices(ticker, start, end)
        bench_df = fetch_prices(benchmark, start, end)

        if stock_df is None or stock_df.empty:
            print(f"  WARNING: No price data for {ticker} (may be delisted)")
            # Try with looser dates or note that delisted stocks need special handling
            results[ticker] = {"error": "No price data (likely delisted)"}
            continue

        if bench_df is None or bench_df.empty:
            print(f"  WARNING: No benchmark data for {benchmark}")
            results[ticker] = {"error": f"No benchmark data for {benchmark}"}
            continue

        # Run event study
        result = run_event_study(
            stock_df[["date", "close"]],
            bench_df[["date", "close"]],
            filing_date,
        )

        if result is None:
            print(f"  Event study failed for {ticker}")
            results[ticker] = {"error": "Event study failed"}
            continue

        results[ticker] = result

        print(f"  Filing Date:    {filing_date}")
        print(f"  Benchmark:      {benchmark}")
        print(f"  Alpha (daily):  {result.get('alpha_daily', 'N/A')}")
        print(f"  Beta:           {result.get('beta', 'N/A')}")
        print(f"  CAR [-30, 0]:   {_fmt(result.get('car_pre30'))}")
        print(f"  CAR [0, +30]:   {_fmt(result.get('car_post30'))}")
        print(f"  CAR [0, +60]:   {_fmt(result.get('car_post60'))}")
        print(f"  CAR [0, +90]:   {_fmt(result.get('car_post90'))}")

        # Sanity check: we expect negative CARs for distressed companies
        car30 = result.get("car_post30")
        if car30 is not None and car30 < -0.05:
            print(f"  PASS: Strong negative CAR confirms distress signal")
        elif car30 is not None and car30 < 0:
            print(f"  PASS: Negative CAR (mild) consistent with distress")
        elif car30 is not None:
            print(f"  NOTE: Positive CAR — filing may have occurred after the main decline")
        else:
            print(f"  SKIP: Could not compute CAR")

    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)

    passed = sum(1 for r in results.values() if isinstance(r, dict) and r.get("car_post30") is not None and r["car_post30"] < 0)
    total = len(ANCHOR_EVENTS)
    print(f"Passed: {passed}/{total} anchors show negative CAR after WARN filing")

    return results


def _fmt(val):
    if val is None:
        return "N/A"
    return f"{val:+.2%}"


if __name__ == "__main__":
    validate()
