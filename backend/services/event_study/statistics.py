"""Statistical analysis for event study results.

Computes cross-sectional statistics, breakdowns, and alpha decay curves
across all WARN filing events.
"""

import logging
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

# Alpha decay evaluation windows (days after event)
ALPHA_DECAY_WINDOWS = [5, 10, 15, 20, 30, 45, 60, 90]


def compute_car_statistics(car_values: List[float]) -> Dict:
    """Compute cross-sectional statistics for a set of CAR values.

    Args:
        car_values: List of CAR values across events

    Returns:
        Dict with mean, median, std, t_stat, p_value, pct_negative, n_events, CI
    """
    arr = np.array([v for v in car_values if v is not None and not np.isnan(v)])

    if len(arr) < 2:
        return {
            "mean": float(arr[0]) if len(arr) == 1 else None,
            "median": float(arr[0]) if len(arr) == 1 else None,
            "std": None,
            "t_stat": None,
            "p_value": None,
            "pct_negative": None,
            "n_events": len(arr),
            "ci_lower": None,
            "ci_upper": None,
        }

    if len(arr) < 50:
        logger.warning(
            f"Sample size {len(arr)} is below 50-event minimum for reliable inference. "
            "Interpret t-stats and p-values with caution."
        )

    t_stat, p_value = stats.ttest_1samp(arr, 0)
    mean = float(np.mean(arr))
    std = float(np.std(arr, ddof=1))

    # 95% confidence interval
    se = std / np.sqrt(len(arr))
    ci_lower = mean - 1.96 * se
    ci_upper = mean + 1.96 * se

    return {
        "mean": mean,
        "median": float(np.median(arr)),
        "std": std,
        "t_stat": float(t_stat),
        "p_value": float(p_value),
        "pct_negative": float(np.mean(arr < 0)),
        "n_events": len(arr),
        "ci_lower": float(ci_lower),
        "ci_upper": float(ci_upper),
    }


def compute_breakdown(
    df: pd.DataFrame,
    group_col: str,
    car_col: str = "car_post30",
) -> Dict[str, Dict]:
    """Compute CAR statistics broken down by a grouping column.

    Args:
        df: DataFrame with event study results
        group_col: Column to group by (e.g., "sector", "market_cap_bucket")
        car_col: CAR column to analyze

    Returns:
        Dict mapping group_value -> statistics dict
    """
    breakdown = {}
    for group, group_df in df.groupby(group_col):
        values = group_df[car_col].dropna().tolist()
        if values:
            breakdown[str(group)] = compute_car_statistics(values)
    return breakdown


def compute_quintile_breakdown(
    df: pd.DataFrame,
    feature_col: str,
    car_col: str = "car_post30",
) -> Dict[str, Dict]:
    """Compute CAR statistics by quintile of a feature.

    Args:
        df: DataFrame with event study results and features
        feature_col: Feature to quintile (e.g., "employees_pct")
        car_col: CAR column to analyze

    Returns:
        Dict mapping quintile label -> statistics dict
    """
    valid = df[[feature_col, car_col]].dropna()
    if len(valid) < 5:
        return {}

    valid["quintile"] = pd.qcut(valid[feature_col], 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"], duplicates="drop")

    breakdown = {}
    for q, q_df in valid.groupby("quintile"):
        values = q_df[car_col].tolist()
        if values:
            breakdown[str(q)] = compute_car_statistics(values)

    return breakdown


def compute_alpha_decay(
    car_timeseries_list: List[List[Dict]],
) -> List[Dict]:
    """Compute alpha decay curve: average CAR at each evaluation window.

    Args:
        car_timeseries_list: List of CAR timeseries (each is list of {day, car} dicts)

    Returns:
        List of {window, mean_car, median_car, ci_lower, ci_upper, n_events}
    """
    decay = []

    for window_days in ALPHA_DECAY_WINDOWS:
        cars_at_window = []

        for ts in car_timeseries_list:
            # Find CAR at the target day
            for point in ts:
                if point["day"] == window_days:
                    cars_at_window.append(point["car"])
                    break
            else:
                # If exact day not found, find closest
                closest = min(
                    [p for p in ts if p["day"] >= 0],
                    key=lambda p: abs(p["day"] - window_days),
                    default=None,
                )
                if closest and abs(closest["day"] - window_days) <= 3:
                    cars_at_window.append(closest["car"])

        if cars_at_window:
            stats_result = compute_car_statistics(cars_at_window)
            decay.append({
                "window": window_days,
                "mean_car": stats_result["mean"],
                "median_car": stats_result["median"],
                "ci_lower": stats_result.get("ci_lower"),
                "ci_upper": stats_result.get("ci_upper"),
                "n_events": stats_result["n_events"],
            })

    return decay


def compute_full_statistics(
    results_df: pd.DataFrame,
    car_timeseries_list: List[List[Dict]],
) -> Dict:
    """Compute comprehensive event study statistics.

    Args:
        results_df: DataFrame with columns:
            car_pre30, car_post30, car_post60, car_post90,
            sector, market_cap_bucket, employees_pct, employees_affected
        car_timeseries_list: List of parsed CAR timeseries

    Returns:
        Dict with all statistics, breakdowns, and alpha decay
    """
    output = {}

    # Overall CAR statistics for each window
    for window in ["car_pre30", "car_post30", "car_post60", "car_post90"]:
        values = results_df[window].dropna().tolist()
        output[window] = compute_car_statistics(values)

    # Breakdowns
    if "sector" in results_df.columns:
        output["sector_breakdown"] = compute_breakdown(results_df, "sector")

    if "market_cap_bucket" in results_df.columns:
        output["cap_breakdown"] = compute_breakdown(results_df, "market_cap_bucket")

    # Quintile breakdowns
    if "employees_pct" in results_df.columns:
        output["employees_pct_quintiles"] = compute_quintile_breakdown(results_df, "employees_pct")

    if "employees_affected" in results_df.columns:
        output["employees_affected_quintiles"] = compute_quintile_breakdown(
            results_df, "employees_affected"
        )

    # Alpha decay
    output["alpha_decay"] = compute_alpha_decay(car_timeseries_list)

    return output
