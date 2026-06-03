"""Tests for CAR calculator."""

import sys
import os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
import pandas as pd
import pytest

from services.event_study.car_calculator import (
    compute_log_returns,
    estimate_market_model,
    compute_abnormal_returns,
    run_event_study,
    MIN_ESTIMATION_DAYS,
)


class TestLogReturns:
    def test_basic_returns(self):
        prices = pd.Series([100.0, 105.0, 110.0])
        returns = compute_log_returns(prices)
        assert len(returns) == 2
        assert abs(returns.iloc[0] - np.log(1.05)) < 1e-10

    def test_returns_length(self):
        prices = pd.Series([100.0] * 10)
        returns = compute_log_returns(prices)
        assert len(returns) == 9


class TestMarketModel:
    def test_estimation_with_known_params(self):
        np.random.seed(42)
        n = 200
        market = np.random.normal(0.001, 0.01, n)
        stock = 0.0002 + 1.5 * market + np.random.normal(0, 0.005, n)

        stock_s = pd.Series(stock, index=range(n))
        market_s = pd.Series(market, index=range(n))

        alpha, beta, _ = estimate_market_model(stock_s, market_s)

        # Beta should be close to 1.5
        assert abs(beta - 1.5) < 0.3, f"Beta {beta} too far from 1.5"
        # Alpha should be close to 0.0002
        assert abs(alpha - 0.0002) < 0.005, f"Alpha {alpha} too far from 0.0002"

    def test_insufficient_data_raises(self):
        stock_s = pd.Series([0.01] * 10, index=range(10))
        market_s = pd.Series([0.01] * 10, index=range(10))

        with pytest.raises(ValueError, match="Insufficient"):
            estimate_market_model(stock_s, market_s)


class TestAbnormalReturns:
    def test_zero_abnormal_returns_when_perfect_model(self):
        np.random.seed(42)
        n = 50
        market = np.random.normal(0.001, 0.01, n)
        alpha = 0.0
        beta = 1.0
        stock = alpha + beta * market  # Perfect model, no residuals

        stock_s = pd.Series(stock, index=range(n))
        market_s = pd.Series(market, index=range(n))

        ar = compute_abnormal_returns(stock_s, market_s, alpha, beta)
        # All abnormal returns should be essentially zero
        assert np.allclose(ar.values, 0, atol=1e-10)


class TestRunEventStudy:
    def test_with_synthetic_data(self, synthetic_prices):
        result = run_event_study(
            synthetic_prices["stock_df"],
            synthetic_prices["market_df"],
            synthetic_prices["event_date"],
        )

        assert result is not None, "Event study should not return None"

        # CAR around event should be negative (we injected -5%)
        assert result["car_post30"] is not None
        # The injected abnormal return is -5%, so CAR should capture this
        # Allow some estimation error
        assert result["car_post30"] < 0, "CAR[0,+30] should be negative after -5% shock"

    def test_returns_all_car_windows(self, synthetic_prices):
        result = run_event_study(
            synthetic_prices["stock_df"],
            synthetic_prices["market_df"],
            synthetic_prices["event_date"],
        )

        assert result is not None
        assert "car_pre30" in result
        assert "car_post30" in result
        assert "car_post60" in result
        assert "car_timeseries" in result
        assert result["alpha_daily"] is not None
        assert result["beta"] is not None

    def test_beta_estimation(self, synthetic_prices):
        result = run_event_study(
            synthetic_prices["stock_df"],
            synthetic_prices["market_df"],
            synthetic_prices["event_date"],
        )

        assert result is not None
        # We set beta=1.2 in synthetic data
        assert abs(result["beta"] - 1.2) < 0.5, f"Estimated beta {result['beta']} too far from 1.2"

    def test_empty_data_returns_none(self):
        empty_df = pd.DataFrame(columns=["date", "close"])
        result = run_event_study(empty_df, empty_df, date(2023, 1, 1))
        assert result is None
