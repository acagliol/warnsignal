"""Backtest engine for the WARN distress signal event study.

Design principles:
- No look-ahead bias: signals fire on filing_date, entry at T+1 open
- Equal weight across open positions
- Configurable holding period and max concurrent positions
- Short-only for WARN distress signals
- Optional borrow cost modeling, variable transaction costs, stop-loss,
  publication lag, and short-selling filters
"""

import logging
from datetime import date, timedelta
from dataclasses import dataclass, field
from typing import List, Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    filing_id: int
    ticker: str
    signal_date: date
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    return_pct: float  # Short return: (entry - exit) / entry
    hold_days: int


@dataclass
class BacktestConfig:
    hold_days: int = 30
    max_positions: int = 20
    min_score: float = 0.0
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    transaction_cost_bps: float = 10.0  # 10 bps per leg
    cap_filter: Optional[List[str]] = None  # e.g. ["micro", "small"]

    # Borrow cost modeling
    borrow_cost_schedule: Dict[str, float] = field(default_factory=lambda: {
        "mega": 25.0,    # 25 bps annualized
        "large": 25.0,   # 25 bps annualized
        "mid": 50.0,     # 50 bps annualized
        "small": 100.0,  # 100 bps annualized
        "micro": 300.0,  # 300 bps annualized (hard to borrow)
    })
    use_borrow_costs: bool = False

    # Variable transaction costs by market cap
    variable_cost_schedule: Dict[str, float] = field(default_factory=lambda: {
        "mega": 5.0,     # 5 bps per leg
        "large": 5.0,
        "mid": 10.0,
        "small": 25.0,
        "micro": 50.0,
    })
    use_variable_costs: bool = False

    # Stop-loss (short position): close if stock rises by this fraction
    stop_loss_pct: Optional[float] = None  # e.g., 0.20 for 20% stop

    # Publication lag: simulate filing -> publication delay
    publication_lag_days: int = 0

    # Short-selling filters
    min_price: float = 0.0       # Minimum stock price to enter short
    min_avg_volume: float = 0.0  # Minimum 20-day average daily volume


@dataclass
class OpenPosition:
    filing_id: int
    ticker: str
    signal_date: date
    entry_date: date
    target_exit_date: date
    entry_price: float
    market_cap_bucket: Optional[str] = None


@dataclass
class BacktestResult:
    trades: List[Trade] = field(default_factory=list)
    equity_curve: List[Dict] = field(default_factory=list)
    config: BacktestConfig = field(default_factory=BacktestConfig)


def validate_no_lookahead(signals_df: pd.DataFrame, prices_df: pd.DataFrame):
    """Validate that no look-ahead bias exists in the signal data.

    Raises AssertionError if any signal uses future data.
    """
    for _, sig in signals_df.iterrows():
        signal_date = sig["signal_date"]

        # Assert: all price data used for scoring must be on or before signal_date
        if "latest_price_date" in sig and pd.notna(sig["latest_price_date"]):
            assert sig["latest_price_date"] <= signal_date, (
                f"Look-ahead bias: signal on {signal_date} uses price from {sig['latest_price_date']}"
            )


def run_backtest(
    signals_df: pd.DataFrame,
    prices_dict: Dict[str, pd.DataFrame],
    config: BacktestConfig = None,
) -> BacktestResult:
    """Run the WARN signal short backtest.

    Args:
        signals_df: DataFrame with columns:
            filing_id, ticker, signal_date, composite_score, sector,
            market_cap_bucket (optional)
        prices_dict: Dict of ticker -> DataFrame with 'date', 'open', 'close',
            and optionally 'volume'
        config: Backtest configuration

    Returns:
        BacktestResult with trades and equity curve
    """
    if config is None:
        config = BacktestConfig()

    result = BacktestResult(config=config)

    # Filter signals by score threshold
    signals = signals_df[signals_df["composite_score"] >= config.min_score].copy()

    # Filter by market cap bucket if specified
    if config.cap_filter:
        cap_lower = [c.lower() for c in config.cap_filter]
        signals = signals[
            signals["market_cap_bucket"].str.lower().isin(cap_lower)
        ].copy()
        logger.info(f"Cap filter {config.cap_filter}: {len(signals)} signals remain")

    # Apply publication lag: shift signal_date forward
    if config.publication_lag_days > 0:
        signals["signal_date"] = signals["signal_date"] + timedelta(
            days=config.publication_lag_days
        )
        logger.info(
            f"Publication lag: shifted signal_date forward by "
            f"{config.publication_lag_days} calendar days"
        )

    signals = signals.sort_values("signal_date")

    if config.start_date:
        signals = signals[signals["signal_date"] >= config.start_date]
    if config.end_date:
        signals = signals[signals["signal_date"] <= config.end_date]

    if signals.empty:
        logger.warning("No signals after filtering")
        return result

    # Build a unified calendar of all trading dates
    all_dates = set()
    for ticker, df in prices_dict.items():
        all_dates.update(df["date"].tolist())
    trading_dates = sorted(all_dates)

    if not trading_dates:
        return result

    # Convert signals to a date-indexed lookup
    signals_by_date = {}
    for _, sig in signals.iterrows():
        d = sig["signal_date"]
        if d not in signals_by_date:
            signals_by_date[d] = []
        signals_by_date[d].append(sig)

    # Sort each date's signals by composite score (descending)
    for d in signals_by_date:
        signals_by_date[d].sort(key=lambda s: s["composite_score"], reverse=True)

    # Simulation
    open_positions: List[OpenPosition] = []
    portfolio_value = 1.0  # Start with $1

    def get_next_trading_day(d: date) -> Optional[date]:
        """Get the next trading day after date d."""
        for td in trading_dates:
            if td > d:
                return td
        return None

    def get_price(ticker: str, d: date, price_field: str = "close") -> Optional[float]:
        """Get price for ticker on date d."""
        if ticker not in prices_dict:
            return None
        df = prices_dict[ticker]
        row = df[df["date"] == d]
        if row.empty:
            return None
        return float(row.iloc[0][price_field])

    def get_avg_volume(ticker: str, as_of_date: date, lookback: int = 20) -> float:
        """Compute average daily volume over the last `lookback` trading days."""
        if ticker not in prices_dict:
            return 0.0
        df = prices_dict[ticker]
        if "volume" not in df.columns:
            return 0.0
        hist = df[df["date"] <= as_of_date].sort_values("date").tail(lookback)
        if hist.empty or hist["volume"].isna().all():
            return 0.0
        return float(hist["volume"].mean())

    def _compute_cost_per_leg(pos: OpenPosition) -> float:
        """Determine the per-leg transaction cost for a position."""
        if config.use_variable_costs:
            cap_bucket = pos.market_cap_bucket or "mid"
            return config.variable_cost_schedule.get(cap_bucket, 10.0) / 10000
        return config.transaction_cost_bps / 10000

    def _close_position(pos: OpenPosition, exit_price: float, exit_date: date):
        """Create a Trade from a closed position."""
        raw_return = (pos.entry_price - exit_price) / pos.entry_price
        cost_per_leg = _compute_cost_per_leg(pos)
        net_return = raw_return - 2 * cost_per_leg  # Entry + exit costs

        trade = Trade(
            filing_id=pos.filing_id,
            ticker=pos.ticker,
            signal_date=pos.signal_date,
            entry_date=pos.entry_date,
            exit_date=exit_date,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            return_pct=net_return,
            hold_days=(exit_date - pos.entry_date).days,
        )

        # LOOK-AHEAD GUARD
        assert trade.entry_date > trade.signal_date, (
            f"Look-ahead bias: entry {trade.entry_date} <= signal {trade.signal_date}"
        )

        return trade

    for idx, current_date in enumerate(trading_dates):
        # 1a. Stop-loss check: close positions where the stock has risen
        #     beyond stop_loss_pct (short is losing money)
        if config.stop_loss_pct is not None:
            stopped_out = []
            for pos in open_positions:
                current_price = get_price(pos.ticker, current_date, "close")
                if current_price is not None and pos.entry_price > 0:
                    price_increase = (current_price - pos.entry_price) / pos.entry_price
                    if price_increase >= config.stop_loss_pct:
                        trade = _close_position(pos, current_price, current_date)
                        result.trades.append(trade)
                        stopped_out.append(pos)
                        logger.debug(
                            f"Stop-loss triggered for {pos.ticker} on {current_date}: "
                            f"entry={pos.entry_price:.2f}, current={current_price:.2f}, "
                            f"rise={price_increase:.1%}"
                        )
            for pos in stopped_out:
                open_positions.remove(pos)

        # 1b. Close positions that have reached their exit date
        positions_to_close = [p for p in open_positions if current_date >= p.target_exit_date]
        for pos in positions_to_close:
            exit_price = get_price(pos.ticker, current_date, "close")
            if exit_price is None:
                # Try next few days
                for offset in range(1, 5):
                    next_d = get_next_trading_day(current_date)
                    if next_d:
                        exit_price = get_price(pos.ticker, next_d, "close")
                        if exit_price:
                            break

            if exit_price is None:
                exit_price = pos.entry_price  # No data = flat

            trade = _close_position(pos, exit_price, current_date)
            result.trades.append(trade)
            open_positions.remove(pos)

        # 2. Check for new signals on the PREVIOUS day (signals fire end-of-day)
        # We check yesterday's signals and enter TODAY at open
        prev_date = None
        if idx > 0:
            prev_date = trading_dates[idx - 1]

        if prev_date and prev_date in signals_by_date and portfolio_value > 0.01:
            new_signals = signals_by_date[prev_date]
            for sig in new_signals:
                if len(open_positions) >= config.max_positions:
                    break

                ticker = sig["ticker"]
                entry_price = get_price(ticker, current_date, "open")
                if entry_price is None or entry_price <= 0:
                    continue

                # Already have a position in this ticker?
                if any(p.ticker == ticker for p in open_positions):
                    continue

                # Short-selling filter: minimum price
                if config.min_price > 0 and entry_price < config.min_price:
                    continue

                # Short-selling filter: minimum average volume
                if config.min_avg_volume > 0:
                    avg_vol = get_avg_volume(ticker, prev_date)
                    if avg_vol < config.min_avg_volume:
                        continue

                # Determine market_cap_bucket for this signal
                sig_cap_bucket = None
                if "market_cap_bucket" in sig.index and pd.notna(sig["market_cap_bucket"]):
                    sig_cap_bucket = str(sig["market_cap_bucket"]).lower()

                # Compute exit date
                target_exit = current_date
                days_added = 0
                for td in trading_dates:
                    if td > current_date:
                        days_added += 1
                        if days_added >= config.hold_days:
                            target_exit = td
                            break
                else:
                    target_exit = current_date + timedelta(days=config.hold_days * 2)

                pos = OpenPosition(
                    filing_id=int(sig["filing_id"]),
                    ticker=ticker,
                    signal_date=prev_date,
                    entry_date=current_date,
                    target_exit_date=target_exit,
                    entry_price=entry_price,
                    market_cap_bucket=sig_cap_bucket,
                )

                open_positions.append(pos)

        # 3. Compute daily portfolio return and update cumulative value
        n_positions = len(open_positions)
        daily_return = 0.0

        if n_positions > 0:
            weight = 1.0 / n_positions
            for pos in open_positions:
                current_price = get_price(pos.ticker, current_date, "close")
                if current_price is not None:
                    # Get previous day price for day-over-day return
                    prev_price = None
                    if idx > 0:
                        prev_price = get_price(pos.ticker, trading_dates[idx - 1], "close")
                    if prev_price is None:
                        prev_price = pos.entry_price

                    # Short return: price going down = positive return
                    if prev_price > 0:
                        day_ret = (prev_price - current_price) / prev_price
                        daily_return += weight * day_ret

                # Deduct daily borrow cost
                if config.use_borrow_costs:
                    cap_bucket = pos.market_cap_bucket or "mid"
                    annual_bps = config.borrow_cost_schedule.get(cap_bucket, 50.0)
                    daily_borrow_cost = annual_bps / 10000 / 252
                    daily_return -= weight * daily_borrow_cost

            # Update cumulative portfolio value
            portfolio_value *= (1 + daily_return)
            # Liquidation floor: if portfolio is effectively wiped out, stop trading
            if portfolio_value < 0.01:
                portfolio_value = 0.01
                logger.warning(f"Portfolio liquidated on {current_date}, stopping new entries")

        result.equity_curve.append({
            "date": current_date.isoformat(),
            "value": round(portfolio_value, 6),
            "n_positions": n_positions,
        })

    # Close any remaining open positions at last available price
    last_date = trading_dates[-1] if trading_dates else None
    for pos in open_positions:
        exit_price = get_price(pos.ticker, last_date, "close") if last_date else pos.entry_price
        if exit_price is None:
            exit_price = pos.entry_price

        trade = _close_position(pos, exit_price, last_date or pos.entry_date)
        result.trades.append(trade)

    return result
