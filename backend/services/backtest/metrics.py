"""Backtest performance metrics.

Computes: Sharpe ratio, max drawdown, win rate, average return,
total return, and other portfolio statistics.
"""

import logging
from typing import List, Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_sharpe_ratio(
    returns: List[float],
    risk_free_rate: float = 0.0,
    annualization_factor: float = 252,
) -> Optional[float]:
    """Compute annualized Sharpe ratio from a series of periodic returns."""
    if len(returns) < 2:
        return None

    arr = np.array(returns)
    excess = arr - risk_free_rate / annualization_factor
    mean_excess = np.mean(excess)
    std = np.std(excess, ddof=1)

    if std == 0 or np.isnan(std):
        return None

    return float(mean_excess / std * np.sqrt(annualization_factor))


def compute_max_drawdown(equity_curve: List[Dict]) -> Optional[float]:
    """Compute maximum drawdown from equity curve.

    Returns drawdown as a positive fraction (e.g., 0.15 = 15% drawdown).
    """
    if not equity_curve:
        return None

    values = [p["value"] for p in equity_curve]
    if not values:
        return None

    peak = values[0]
    max_dd = 0.0

    for v in values:
        if v > peak:
            peak = v
        dd = (peak - v) / peak if peak > 0 else 0
        max_dd = max(max_dd, dd)

    return float(max_dd)


def compute_win_rate(trade_returns: List[float]) -> Optional[float]:
    """Compute win rate (fraction of profitable trades)."""
    if not trade_returns:
        return None
    wins = sum(1 for r in trade_returns if r > 0)
    return float(wins / len(trade_returns))


def compute_metrics(trades: List, equity_curve: List[Dict]) -> Dict:
    """Compute all backtest metrics from trades and equity curve.

    Args:
        trades: List of Trade objects with return_pct attribute
        equity_curve: List of {date, value} dicts

    Returns:
        Dict with all metrics
    """
    trade_returns = [t.return_pct for t in trades]

    # Build daily returns from equity curve
    daily_returns = []
    if len(equity_curve) >= 2:
        for i in range(1, len(equity_curve)):
            prev = equity_curve[i - 1]["value"]
            curr = equity_curve[i]["value"]
            if prev > 0:
                daily_returns.append((curr - prev) / prev)

    total_return = None
    if equity_curve:
        start = equity_curve[0]["value"]
        end = equity_curve[-1]["value"]
        if start > 0:
            total_return = (end - start) / start

    return {
        "sharpe_ratio": compute_sharpe_ratio(daily_returns),
        "max_drawdown": compute_max_drawdown(equity_curve),
        "win_rate": compute_win_rate(trade_returns),
        "total_return": total_return,
        "n_trades": len(trades),
        "avg_return": float(np.mean(trade_returns)) if trade_returns else None,
        "median_return": float(np.median(trade_returns)) if trade_returns else None,
        "avg_hold_days": float(np.mean([t.hold_days for t in trades])) if trades else None,
        "total_days": (
            len(equity_curve) if equity_curve else 0
        ),
    }
