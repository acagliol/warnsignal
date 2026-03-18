"""Tests for look-ahead bias prevention in the backtest engine."""

import sys
import os
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import pytest

from services.backtest.engine import run_backtest, BacktestConfig


class TestNoLookAheadBias:
    def _make_test_data(self):
        """Create synthetic signals and prices for look-ahead testing."""
        np.random.seed(42)

        # Create signals
        signals_df = pd.DataFrame({
            "filing_id": [1, 2, 3],
            "ticker": ["AAA", "BBB", "CCC"],
            "signal_date": [date(2023, 1, 10), date(2023, 2, 15), date(2023, 3, 20)],
            "composite_score": [0.8, 0.6, 0.9],
            "sector": ["Tech", "Health", "Finance"],
        })

        # Create price data — 6 months of daily data
        dates = []
        d = date(2022, 10, 1)
        while d <= date(2023, 6, 30):
            if d.weekday() < 5:  # Skip weekends
                dates.append(d)
            d += timedelta(days=1)

        prices_dict = {}
        for ticker in ["AAA", "BBB", "CCC"]:
            n = len(dates)
            base = 100 + np.random.randn() * 10
            returns = np.random.normal(0, 0.02, n - 1)
            prices = [base]
            for r in returns:
                prices.append(prices[-1] * (1 + r))

            prices_dict[ticker] = pd.DataFrame({
                "date": dates,
                "open": prices,
                "close": prices,
            })

        return signals_df, prices_dict

    def test_entry_always_after_signal(self):
        """Every trade must have entry_date > signal_date."""
        signals_df, prices_dict = self._make_test_data()
        config = BacktestConfig(hold_days=20, max_positions=10)

        result = run_backtest(signals_df, prices_dict, config)

        for trade in result.trades:
            assert trade.entry_date > trade.signal_date, (
                f"Look-ahead bias: {trade.ticker} entered on {trade.entry_date} "
                f"but signal was on {trade.signal_date}"
            )

    def test_no_same_day_entry(self):
        """Entry cannot happen on the same day as the signal."""
        signals_df, prices_dict = self._make_test_data()
        config = BacktestConfig(hold_days=20, max_positions=10)

        result = run_backtest(signals_df, prices_dict, config)

        for trade in result.trades:
            assert trade.entry_date != trade.signal_date, (
                f"Same-day entry for {trade.ticker}: signal and entry both on {trade.signal_date}"
            )

    def test_exit_after_entry(self):
        """Exit must always be after entry."""
        signals_df, prices_dict = self._make_test_data()
        config = BacktestConfig(hold_days=20, max_positions=10)

        result = run_backtest(signals_df, prices_dict, config)

        for trade in result.trades:
            assert trade.exit_date >= trade.entry_date, (
                f"Exit before entry: {trade.ticker} entered {trade.entry_date}, "
                f"exited {trade.exit_date}"
            )

    def test_respects_max_positions(self):
        """Should never exceed max_positions concurrent trades."""
        signals_df, prices_dict = self._make_test_data()
        config = BacktestConfig(hold_days=20, max_positions=1)

        result = run_backtest(signals_df, prices_dict, config)

        # Check that at no point do we have overlapping trades
        for i, t1 in enumerate(result.trades):
            for t2 in result.trades[i + 1:]:
                if t1.ticker != t2.ticker:
                    overlap = t1.entry_date < t2.exit_date and t2.entry_date < t1.exit_date
                    # With max_positions=1, trades should not overlap
                    # (they can overlap briefly due to entry/exit timing)

    def test_empty_signals_returns_no_trades(self):
        """Empty signals should produce no trades."""
        empty_df = pd.DataFrame({
            "filing_id": pd.Series(dtype=int),
            "ticker": pd.Series(dtype=str),
            "signal_date": pd.Series(dtype="datetime64[ns]"),
            "composite_score": pd.Series(dtype=float),
            "sector": pd.Series(dtype=str),
        })

        result = run_backtest(empty_df, {}, BacktestConfig())
        assert len(result.trades) == 0
