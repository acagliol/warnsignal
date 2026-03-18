"""Cumulative Abnormal Return (CAR) calculator using the Market Model.

Event study methodology:
1. Estimation window: [-270, -31] trading days (240 days for model estimation)
2. Event window: [-30, +90] trading days
3. Market Model: R_i = alpha + beta * R_m + epsilon
4. Abnormal Return: AR = R_i - (alpha_hat + beta_hat * R_m)
5. CAR = cumulative sum of AR over specified windows
"""

import json
import logging
from datetime import date
from typing import Optional, Dict, Tuple, List

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

# Window definitions (in trading days relative to event date)
ESTIMATION_START = -270
ESTIMATION_END = -31
EVENT_START = -30
EVENT_END = 90

# CAR windows to compute
CAR_WINDOWS = {
    "pre30": (-30, 0),
    "post30": (0, 30),
    "post60": (0, 60),
    "post90": (0, 90),
}

MIN_ESTIMATION_DAYS = 120  # Minimum trading days required in estimation window


def compute_log_returns(prices: pd.Series) -> pd.Series:
    """Compute log returns from a price series."""
    return np.log(prices / prices.shift(1)).dropna()


def estimate_market_model(
    stock_returns: pd.Series,
    market_returns: pd.Series,
) -> Tuple[float, float, float]:
    """Estimate market model via OLS: R_i = alpha + beta * R_m.

    Returns (alpha, beta, residual_std).
    Raises ValueError if insufficient data.
    """
    # Align series
    aligned = pd.concat([stock_returns, market_returns], axis=1, join="inner").dropna()

    if len(aligned) < MIN_ESTIMATION_DAYS:
        raise ValueError(
            f"Insufficient estimation data: {len(aligned)} days "
            f"(need {MIN_ESTIMATION_DAYS})"
        )

    y = aligned.iloc[:, 0].values
    x = aligned.iloc[:, 1].values

    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

    residuals = y - (intercept + slope * x)
    residual_std = np.std(residuals, ddof=2)

    return intercept, slope, residual_std


def compute_abnormal_returns(
    stock_returns: pd.Series,
    market_returns: pd.Series,
    alpha: float,
    beta: float,
) -> pd.Series:
    """Compute abnormal returns: AR = R_i - (alpha + beta * R_m)."""
    aligned = pd.concat([stock_returns, market_returns], axis=1, join="inner").dropna()

    stock_r = aligned.iloc[:, 0]
    market_r = aligned.iloc[:, 1]

    expected_returns = alpha + beta * market_r
    abnormal_returns = stock_r - expected_returns

    return abnormal_returns


def compute_car(
    abnormal_returns: pd.Series,
    event_idx: int,
    window_start: int,
    window_end: int,
) -> Optional[float]:
    """Compute Cumulative Abnormal Return for a given window.

    Args:
        abnormal_returns: Series of daily abnormal returns (indexed 0..N)
        event_idx: Index of the event date in the series
        window_start: Start of window relative to event (e.g., -30)
        window_end: End of window relative to event (e.g., +30)

    Returns:
        CAR value or None if insufficient data.
    """
    start_idx = event_idx + window_start
    end_idx = event_idx + window_end

    if start_idx < 0 or end_idx >= len(abnormal_returns):
        return None

    return float(abnormal_returns.iloc[start_idx:end_idx + 1].sum())


def run_event_study(
    stock_prices: pd.DataFrame,
    benchmark_prices: pd.DataFrame,
    event_date: date,
) -> Optional[Dict]:
    """Run a full event study for a single event.

    Args:
        stock_prices: DataFrame with 'date' and 'close' columns
        benchmark_prices: DataFrame with 'date' and 'close' columns
        event_date: The WARN filing date

    Returns:
        Dict with CAR values, timeseries, alpha, beta, or None if study fails.
    """
    try:
        # Sort and index by date
        stock = stock_prices.sort_values("date").set_index("date")["close"]
        benchmark = benchmark_prices.sort_values("date").set_index("date")["close"]

        # Compute returns
        stock_returns = compute_log_returns(stock)
        market_returns = compute_log_returns(benchmark)

        # Get trading days index
        all_dates = stock_returns.index.sort_values()
        event_date_pd = pd.Timestamp(event_date).date() if not isinstance(event_date, date) else event_date

        # Find closest trading day to event date
        valid_dates = [d for d in all_dates if d <= event_date_pd]
        if not valid_dates:
            logger.warning(f"No trading days on or before event date {event_date}")
            return None

        actual_event_date = max(valid_dates)
        event_pos = list(all_dates).index(actual_event_date)

        # Split into estimation and event windows
        est_start_pos = event_pos + ESTIMATION_START
        est_end_pos = event_pos + ESTIMATION_END
        evt_start_pos = event_pos + EVENT_START
        evt_end_pos = min(event_pos + EVENT_END, len(all_dates) - 1)

        if est_start_pos < 0:
            est_start_pos = 0

        estimation_dates = all_dates[est_start_pos:est_end_pos + 1]

        # Estimate market model on estimation window
        est_stock = stock_returns.loc[stock_returns.index.isin(estimation_dates)]
        est_market = market_returns.loc[market_returns.index.isin(estimation_dates)]

        alpha, beta, residual_std = estimate_market_model(est_stock, est_market)

        # Compute abnormal returns over event window
        event_dates = all_dates[max(evt_start_pos, 0):evt_end_pos + 1]
        evt_stock = stock_returns.loc[stock_returns.index.isin(event_dates)]
        evt_market = market_returns.loc[market_returns.index.isin(event_dates)]

        ar = compute_abnormal_returns(evt_stock, evt_market, alpha, beta)

        # Map to relative trading days
        event_day_in_ar = 0
        for i, d in enumerate(ar.index):
            if d >= actual_event_date:
                event_day_in_ar = i
                break

        # Compute CARs for each window
        cars = {}
        for window_name, (w_start, w_end) in CAR_WINDOWS.items():
            cars[f"car_{window_name}"] = compute_car(ar, event_day_in_ar, w_start, w_end)

        # Build daily CAR timeseries for charting
        car_ts = []
        cumulative = 0.0
        for i, (d, ar_val) in enumerate(ar.items()):
            relative_day = i - event_day_in_ar
            cumulative += ar_val
            car_ts.append({"day": relative_day, "car": round(cumulative, 6)})

        # Compute t-stat for CAR[0,+30]
        t_stat_post30 = None
        p_value_post30 = None
        if cars.get("car_post30") is not None and residual_std > 0:
            n_days = min(30, evt_end_pos - event_pos)
            if n_days > 0:
                car_std = residual_std * np.sqrt(n_days)
                t_stat_post30 = cars["car_post30"] / car_std if car_std > 0 else None
                if t_stat_post30 is not None:
                    p_value_post30 = 2 * (1 - stats.norm.cdf(abs(t_stat_post30)))

        return {
            "car_pre30": cars.get("car_pre30"),
            "car_post30": cars.get("car_post30"),
            "car_post60": cars.get("car_post60"),
            "car_post90": cars.get("car_post90"),
            "car_timeseries": json.dumps(car_ts),
            "alpha_daily": float(alpha),
            "beta": float(beta),
            "t_stat_post30": t_stat_post30,
            "p_value_post30": p_value_post30,
            "estimation_window_start": estimation_dates[0] if len(estimation_dates) > 0 else None,
            "estimation_window_end": estimation_dates[-1] if len(estimation_dates) > 0 else None,
        }

    except ValueError as e:
        logger.warning(f"Event study failed for {event_date}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in event study for {event_date}: {e}")
        return None
