"""Tests for signal scorer."""

import sys
import os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np

from services.signal.scorer import (
    compute_filing_lead_days,
    check_repeat_filer,
    percentile_rank,
    score_signals,
)


class TestFilingLeadDays:
    def test_normal_case(self):
        result = compute_filing_lead_days(date(2023, 1, 1), date(2023, 2, 1))
        assert result == 31

    def test_same_day(self):
        result = compute_filing_lead_days(date(2023, 1, 1), date(2023, 1, 1))
        assert result == 0

    def test_none_filing(self):
        result = compute_filing_lead_days(None, date(2023, 1, 1))
        assert result is None

    def test_none_layoff(self):
        result = compute_filing_lead_days(date(2023, 1, 1), None)
        assert result is None

    def test_negative_clamps_to_zero(self):
        # Layoff before filing (shouldn't happen but handle gracefully)
        result = compute_filing_lead_days(date(2023, 2, 1), date(2023, 1, 1))
        assert result == 0


class TestRepeatFiler:
    def test_repeat_filer_found(self):
        prior = [
            {"ticker": "BBBY", "filing_date": date(2022, 6, 1)},
            {"ticker": "BBBY", "filing_date": date(2023, 1, 1)},
        ]
        result = check_repeat_filer("BBBY", date(2023, 6, 1), prior)
        assert result is True

    def test_no_repeat(self):
        prior = [
            {"ticker": "BBBY", "filing_date": date(2021, 1, 1)},
        ]
        result = check_repeat_filer("BBBY", date(2023, 6, 1), prior)
        assert result is False

    def test_different_ticker(self):
        prior = [
            {"ticker": "AAPL", "filing_date": date(2023, 3, 1)},
        ]
        result = check_repeat_filer("BBBY", date(2023, 6, 1), prior)
        assert result is False

    def test_empty_prior(self):
        result = check_repeat_filer("BBBY", date(2023, 6, 1), [])
        assert result is False


class TestPercentileRank:
    def test_basic_ranking(self):
        values = pd.Series([10, 20, 30, 40, 50])
        ranks = percentile_rank(values)
        assert ranks.iloc[0] == 0.2  # Lowest
        assert ranks.iloc[-1] == 1.0  # Highest


class TestScoreSignals:
    def test_scoring_produces_valid_scores(self):
        df = pd.DataFrame({
            "ticker": ["BBBY", "RAD", "PRTY"],
            "filing_date": [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)],
            "layoff_date": [date(2023, 2, 1), date(2023, 3, 1), date(2023, 4, 1)],
            "employees_affected": [500, 200, 300],
            "total_employees": [5000, 10000, 2000],
            "sector": ["Consumer Discretionary", "Consumer Staples", "Consumer Discretionary"],
            "market_cap_bucket": ["small", "mid", "small"],
            "repeat_filer": [True, False, False],
        })

        scored = score_signals(df)

        assert "composite_score" in scored.columns
        assert all(scored["composite_score"] >= 0)
        assert all(scored["composite_score"] <= 1)
        assert len(scored) == 3

    def test_higher_pct_gets_higher_score(self):
        df = pd.DataFrame({
            "ticker": ["A", "B"],
            "filing_date": [date(2023, 1, 1)] * 2,
            "layoff_date": [date(2023, 2, 1)] * 2,
            "employees_affected": [100, 100],
            "total_employees": [200, 10000],  # A has 50%, B has 1%
            "sector": ["Industrials"] * 2,
            "market_cap_bucket": ["mid"] * 2,
            "repeat_filer": [False, False],
        })

        scored = score_signals(df)
        score_a = scored[scored["ticker"] == "A"]["composite_score"].iloc[0]
        score_b = scored[scored["ticker"] == "B"]["composite_score"].iloc[0]
        assert score_a > score_b, "Higher employee % should yield higher score"
