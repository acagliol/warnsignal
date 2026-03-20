"""Statistical analysis for event study results.

Computes cross-sectional statistics, breakdowns, and alpha decay curves
across all WARN filing events.  Includes bootstrap inference,
multiple-testing corrections, non-parametric tests, and placebo
(randomisation) tests for robustness.
"""

import logging
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

# Alpha decay evaluation windows (days after event)
ALPHA_DECAY_WINDOWS = [5, 10, 15, 20, 30, 45, 60, 90]


# ---------------------------------------------------------------------------
# Advanced statistical helpers
# ---------------------------------------------------------------------------

def bootstrap_car_ci(
    car_values: List[float],
    n_bootstrap: int = 10000,
    ci: float = 0.95,
) -> Dict:
    """Compute bootstrap confidence intervals for mean CAR.

    More robust than normal-theory CI for skewed distributions.
    Uses the percentile method: the CI bounds are the (alpha/2) and
    (1 - alpha/2) percentiles of the bootstrap mean distribution.

    Args:
        car_values: Raw CAR values across events.
        n_bootstrap: Number of bootstrap resamples.
        ci: Confidence level (default 0.95 for a 95 % CI).

    Returns:
        {"mean": float, "ci_lower": float, "ci_upper": float,
         "se_bootstrap": float}
    """
    arr = np.array([v for v in car_values if v is not None and not np.isnan(v)])

    if len(arr) == 0:
        return {"mean": None, "ci_lower": None, "ci_upper": None, "se_bootstrap": None}
    if len(arr) == 1:
        return {
            "mean": float(arr[0]),
            "ci_lower": float(arr[0]),
            "ci_upper": float(arr[0]),
            "se_bootstrap": 0.0,
        }

    rng = np.random.default_rng(seed=42)
    # shape: (n_bootstrap, n_samples)
    indices = rng.integers(0, len(arr), size=(n_bootstrap, len(arr)))
    boot_means = arr[indices].mean(axis=1)

    alpha = 1.0 - ci
    lower = float(np.percentile(boot_means, 100 * alpha / 2))
    upper = float(np.percentile(boot_means, 100 * (1 - alpha / 2)))

    return {
        "mean": float(np.mean(arr)),
        "ci_lower": lower,
        "ci_upper": upper,
        "se_bootstrap": float(np.std(boot_means, ddof=1)),
    }


def correct_pvalues(p_values: List[float], method: str = "bh") -> List[float]:
    """Apply multiple-testing correction to a list of p-values.

    Methods:
        "bonferroni": p_corrected = min(p * n_tests, 1.0)
        "bh": Benjamini-Hochberg procedure controlling the False Discovery
              Rate (FDR).

    Args:
        p_values: Raw (uncorrected) p-values.
        method: "bonferroni" or "bh".

    Returns:
        List of corrected p-values in the same order as the input.
    """
    if not p_values:
        return []

    # Filter out None values, keeping track of positions
    indexed = [(i, p) for i, p in enumerate(p_values) if p is not None]
    if not indexed:
        return [None] * len(p_values)  # type: ignore[list-item]

    positions, raw = zip(*indexed)
    raw = list(raw)
    n = len(raw)

    if method == "bonferroni":
        corrected_vals = [min(p * n, 1.0) for p in raw]

    elif method == "bh":
        # Benjamini-Hochberg step-up procedure
        order = np.argsort(raw)
        corrected_vals = [0.0] * n
        cum_min = 1.0
        for rank_from_end, idx in enumerate(reversed(order)):
            rank = n - rank_from_end  # 1-based rank
            adj = raw[idx] * n / rank
            cum_min = min(cum_min, adj)
            corrected_vals[idx] = min(cum_min, 1.0)
    else:
        raise ValueError(f"Unknown correction method: {method!r}. Use 'bonferroni' or 'bh'.")

    # Re-insert into original positions (preserving None slots)
    result: List[Optional[float]] = [None] * len(p_values)
    for pos, val in zip(positions, corrected_vals):
        result[pos] = float(val)
    return result  # type: ignore[return-value]


def compute_nonparametric_tests(car_values: List[float]) -> Dict:
    """Run non-parametric significance tests on CAR values.

    Returns:
        - wilcoxon_stat, wilcoxon_p: Wilcoxon signed-rank test (H0: median = 0)
        - sign_test_p: Binomial sign test (H0: fraction negative <= 50 %)
        - skewness, kurtosis: Distribution shape descriptors
    """
    arr = np.array([v for v in car_values if v is not None and not np.isnan(v)])

    empty_result: Dict = {
        "wilcoxon_stat": None,
        "wilcoxon_p": None,
        "sign_test_p": None,
        "skewness": None,
        "kurtosis": None,
    }

    if len(arr) < 2:
        return empty_result

    # Skewness and kurtosis (Fisher definition, excess kurtosis)
    skewness = float(stats.skew(arr, bias=False))
    kurtosis = float(stats.kurtosis(arr, bias=False))

    # Wilcoxon signed-rank test — requires non-zero differences
    nonzero = arr[arr != 0]
    if len(nonzero) >= 10:
        try:
            w_stat, w_p = stats.wilcoxon(nonzero, alternative="two-sided")
            w_stat, w_p = float(w_stat), float(w_p)
        except ValueError:
            w_stat, w_p = None, None
    else:
        w_stat, w_p = None, None

    # Sign test: is the number of negatives significantly > 50 %?
    n_neg = int(np.sum(arr < 0))
    n_total = len(arr)
    sign_result = stats.binomtest(n_neg, n_total, p=0.5, alternative="greater")
    sign_p = float(sign_result.pvalue)

    return {
        "wilcoxon_stat": w_stat,
        "wilcoxon_p": w_p,
        "sign_test_p": sign_p,
        "skewness": skewness,
        "kurtosis": kurtosis,
    }


def placebo_test(
    car_values: List[float],
    n_permutations: int = 1000,
) -> Dict:
    """Randomisation (placebo) test for the mean CAR.

    Shuffles the sign of each CAR value at random to build a null
    distribution of the mean under H0 (no systematic effect).
    Compares the actual mean CAR to this null distribution.

    Args:
        car_values: Observed CAR values.
        n_permutations: Number of random permutations.

    Returns:
        - actual_mean: Observed mean CAR.
        - null_mean, null_std: Centre and spread of the null distribution.
        - percentile_rank: Where the actual mean falls (0-100) in the null.
        - placebo_p_value: Two-sided p-value (fraction of null means at least
          as extreme as the actual mean in absolute value).
    """
    arr = np.array([v for v in car_values if v is not None and not np.isnan(v)])

    empty: Dict = {
        "actual_mean": None,
        "null_mean": None,
        "null_std": None,
        "percentile_rank": None,
        "placebo_p_value": None,
    }

    if len(arr) < 2:
        if len(arr) == 1:
            empty["actual_mean"] = float(arr[0])
        return empty

    rng = np.random.default_rng(seed=123)
    actual_mean = float(np.mean(arr))

    # Generate null distribution by randomly flipping signs
    signs = rng.choice([-1, 1], size=(n_permutations, len(arr)))
    null_means = (signs * arr).mean(axis=1)

    null_mean = float(np.mean(null_means))
    null_std = float(np.std(null_means, ddof=1))
    percentile_rank = float(np.mean(null_means <= actual_mean) * 100)

    # Two-sided p-value
    placebo_p = float(np.mean(np.abs(null_means) >= abs(actual_mean)))

    return {
        "actual_mean": actual_mean,
        "null_mean": null_mean,
        "null_std": null_std,
        "percentile_rank": percentile_rank,
        "placebo_p_value": placebo_p,
    }


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
            "bootstrap_ci": bootstrap_car_ci(car_values),
            **compute_nonparametric_tests(car_values),
        }

    if len(arr) < 50:
        logger.warning(
            f"Sample size {len(arr)} is below 50-event minimum for reliable inference. "
            "Interpret t-stats and p-values with caution."
        )

    t_stat, p_value = stats.ttest_1samp(arr, 0)
    mean = float(np.mean(arr))
    std = float(np.std(arr, ddof=1))

    # 95% confidence interval (normal theory)
    se = std / np.sqrt(len(arr))
    ci_lower = mean - 1.96 * se
    ci_upper = mean + 1.96 * se

    # Advanced statistics
    boot = bootstrap_car_ci(car_values)
    nonparam = compute_nonparametric_tests(car_values)

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
        "bootstrap_ci": boot,
        **nonparam,
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

    # Overall CAR statistics for each window (now includes bootstrap CI
    # and non-parametric tests via compute_car_statistics)
    car_windows = ["car_pre30", "car_post30", "car_post60", "car_post90"]
    for window in car_windows:
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

    # Sub-sample analysis — targeted subsets where the signal should be strongest
    output["subsample"] = compute_subsample_analysis(results_df)

    # ------------------------------------------------------------------
    # Multiple-testing correction across all window + sub-sample p-values
    # ------------------------------------------------------------------
    all_p_labels: List[str] = []
    all_p_values: List[Optional[float]] = []

    # Collect p-values from main windows
    for window in car_windows:
        p = output[window].get("p_value")
        all_p_labels.append(window)
        all_p_values.append(p)

    # Collect p-values from sub-sample analyses
    for ss_name, ss_data in output.get("subsample", {}).items():
        for w in ["car_post30", "car_post60", "car_post90"]:
            if w in ss_data and isinstance(ss_data[w], dict):
                p = ss_data[w].get("p_value")
                all_p_labels.append(f"subsample:{ss_name}:{w}")
                all_p_values.append(p)

    corrected_bh = correct_pvalues(all_p_values, method="bh")
    corrected_bonf = correct_pvalues(all_p_values, method="bonferroni")

    output["corrected_pvalues"] = {
        label: {"raw": raw, "bh": bh, "bonferroni": bonf}
        for label, raw, bh, bonf in zip(
            all_p_labels, all_p_values, corrected_bh, corrected_bonf
        )
    }

    # ------------------------------------------------------------------
    # Placebo (randomisation) test for the primary CAR[0,+30] window
    # ------------------------------------------------------------------
    post30_values = results_df["car_post30"].dropna().tolist()
    output["placebo_test_post30"] = placebo_test(post30_values)

    return output


def compute_subsample_analysis(df: pd.DataFrame) -> Dict[str, Dict]:
    """Compute CAR statistics for targeted sub-samples.

    These sub-samples test the market microstructure thesis:
    the signal should be strongest where analyst coverage is thinnest.
    """
    subsamples = {}

    # Micro + Small cap only (thinnest coverage)
    if "market_cap_bucket" in df.columns:
        micro_small = df[df["market_cap_bucket"].isin(["micro", "small"])]
        if len(micro_small) >= 5:
            subsamples["Micro + Small Cap"] = {
                "filter": "market_cap_bucket in [micro, small]",
                "n_events": len(micro_small),
                **{w: compute_car_statistics(micro_small[w].dropna().tolist())
                   for w in ["car_post30", "car_post60", "car_post90"]
                   if w in micro_small.columns},
            }

    # Exclude mega-cap (where signal is noise)
    if "market_cap_bucket" in df.columns:
        no_mega = df[df["market_cap_bucket"] != "mega"]
        if len(no_mega) >= 10:
            subsamples["Exclude Mega-Cap"] = {
                "filter": "market_cap_bucket != mega",
                "n_events": len(no_mega),
                **{w: compute_car_statistics(no_mega[w].dropna().tolist())
                   for w in ["car_post30", "car_post60", "car_post90"]
                   if w in no_mega.columns},
            }

    # Exclude Technology sector (where layoffs are often bullish)
    if "sector" in df.columns:
        no_tech = df[~df["sector"].isin(["Technology", "Information Technology"])]
        if len(no_tech) >= 10:
            subsamples["Exclude Technology"] = {
                "filter": "sector != Technology",
                "n_events": len(no_tech),
                **{w: compute_car_statistics(no_tech[w].dropna().tolist())
                   for w in ["car_post30", "car_post60", "car_post90"]
                   if w in no_tech.columns},
            }

    # Healthcare only (strongest signal in current data)
    if "sector" in df.columns:
        healthcare = df[df["sector"].isin(["Healthcare", "Health Care"])]
        if len(healthcare) >= 5:
            subsamples["Healthcare Only"] = {
                "filter": "sector in [Healthcare]",
                "n_events": len(healthcare),
                **{w: compute_car_statistics(healthcare[w].dropna().tolist())
                   for w in ["car_post30", "car_post60", "car_post90"]
                   if w in healthcare.columns},
            }

    # Exclude mega-cap AND tech (cleanest signal)
    if "market_cap_bucket" in df.columns and "sector" in df.columns:
        clean = df[
            (df["market_cap_bucket"] != "mega") &
            (~df["sector"].isin(["Technology", "Information Technology"]))
        ]
        if len(clean) >= 10:
            subsamples["Excl Mega-Cap + Tech"] = {
                "filter": "market_cap_bucket != mega AND sector != Technology",
                "n_events": len(clean),
                **{w: compute_car_statistics(clean[w].dropna().tolist())
                   for w in ["car_post30", "car_post60", "car_post90"]
                   if w in clean.columns},
            }

    return subsamples
