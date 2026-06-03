"""Signal scorer: compute composite distress score for each WARN filing.

Features:
- employees_affected: raw count of affected employees
- employees_pct: affected as % of total company headcount
- filing_lead_days: days between filing date and layoff date (more = more distress)
- repeat_filer: company filed WARN in prior 12 months
- sector: GICS sector (used for sector-specific adjustment)
- market_cap_bucket: size category

Composite score = weighted sum of percentile-ranked features.
"""

import logging
from datetime import date, timedelta
from typing import List, Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Composite score weights
WEIGHTS = {
    "employees_pct": 0.30,
    "employees_affected": 0.25,
    "repeat_filer": 0.20,
    "filing_lead_days": 0.15,
    "sector_factor": 0.10,
}

# Historical sector CARs (placeholder; updated from actual backtest results)
# Higher = sector where WARN signals are more predictive
DEFAULT_SECTOR_FACTORS = {
    "Consumer Discretionary": 0.8,
    "Consumer Staples": 0.5,
    "Information Technology": 0.6,
    "Health Care": 0.5,
    "Financials": 0.4,
    "Industrials": 0.7,
    "Energy": 0.6,
    "Materials": 0.7,
    "Communication Services": 0.5,
    "Utilities": 0.3,
    "Real Estate": 0.6,
}


def compute_filing_lead_days(filing_date: Optional[date], layoff_date: Optional[date]) -> Optional[int]:
    """Compute days between filing date and layoff date."""
    if filing_date is None or layoff_date is None:
        return None
    delta = (layoff_date - filing_date).days
    return max(delta, 0)


def check_repeat_filer(
    ticker: str,
    filing_date: date,
    prior_filings: List[Dict],
) -> bool:
    """Check if this company filed WARN in prior 12 months."""
    cutoff = filing_date - timedelta(days=365)
    for pf in prior_filings:
        if (
            pf.get("ticker") == ticker
            and pf.get("filing_date") is not None
            and cutoff <= pf["filing_date"] < filing_date
        ):
            return True
    return False


def percentile_rank(values: pd.Series) -> pd.Series:
    """Compute percentile ranks (0-1) for a series."""
    return values.rank(pct=True, na_option="bottom")


def score_signals(filings_df: pd.DataFrame, sector_factors: Optional[Dict] = None) -> pd.DataFrame:
    """Score all filings and compute composite signal score.

    Args:
        filings_df: DataFrame with columns:
            ticker, filing_date, layoff_date, employees_affected,
            total_employees, sector, market_cap_bucket, repeat_filer
        sector_factors: Optional dict of sector -> factor (0-1)

    Returns:
        DataFrame with added 'composite_score' column and all features
    """
    if sector_factors is None:
        sector_factors = DEFAULT_SECTOR_FACTORS

    df = filings_df.copy()

    # Compute features
    df["filing_lead_days"] = df.apply(
        lambda r: compute_filing_lead_days(r.get("filing_date"), r.get("layoff_date")),
        axis=1,
    )

    df["employees_pct"] = df.apply(
        lambda r: (
            r["employees_affected"] / r["total_employees"] * 100
            if pd.notna(r.get("employees_affected")) and pd.notna(r.get("total_employees")) and r.get("total_employees", 0) > 0
            else None
        ),
        axis=1,
    )

    # Percentile rank features
    rank_cols = ["employees_affected", "employees_pct", "filing_lead_days"]
    for col in rank_cols:
        df[f"{col}_rank"] = percentile_rank(df[col].astype(float))

    # Sector factor
    df["sector_factor"] = df["sector"].map(sector_factors).fillna(0.5)

    # Repeat filer is already boolean (0 or 1)
    df["repeat_filer_score"] = df["repeat_filer"].astype(float)

    # Composite score
    df["composite_score"] = (
        WEIGHTS["employees_pct"] * df["employees_pct_rank"].fillna(0.5)
        + WEIGHTS["employees_affected"] * df["employees_affected_rank"].fillna(0.5)
        + WEIGHTS["repeat_filer"] * df["repeat_filer_score"]
        + WEIGHTS["filing_lead_days"] * df["filing_lead_days_rank"].fillna(0.5)
        + WEIGHTS["sector_factor"] * df["sector_factor"]
    )

    # Normalize to 0-1 range
    score_min = df["composite_score"].min()
    score_max = df["composite_score"].max()
    if score_max > score_min:
        df["composite_score"] = (df["composite_score"] - score_min) / (score_max - score_min)

    return df
