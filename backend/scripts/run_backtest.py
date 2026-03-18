"""CLI: Run the WARN signal backtest simulation."""

import sys
import os
import json
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import math
import pandas as pd
import numpy as np
from database import engine, SessionLocal, Base
from models import WarnFiling, EntityMatch, PriceData, EventStudyResult, Signal, BacktestRun, BacktestTrade
from services.signal.scorer import score_signals
from services.backtest.engine import run_backtest, BacktestConfig
from services.backtest.metrics import compute_metrics
from services.market_data.price_loader import get_company_info
from config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    # Build signals DataFrame from resolved filings with event studies
    rows = []
    filings = (
        db.query(WarnFiling, EntityMatch, EventStudyResult)
        .join(EntityMatch, WarnFiling.id == EntityMatch.filing_id)
        .join(EventStudyResult, WarnFiling.id == EventStudyResult.filing_id)
        .filter(EntityMatch.ticker.isnot(None))
        .all()
    )

    logger.info(f"Building signals from {len(filings)} filings with event studies")

    # Get all prior filings for repeat_filer check
    all_filings_lookup = [
        {"ticker": e.ticker, "filing_date": f.filing_date}
        for f, e, _ in filings
    ]

    for filing, entity, es in filings:
        # Get total employees from yfinance (cached)
        info = get_company_info(entity.ticker)
        total_employees = info.get("full_time_employees")

        from services.signal.scorer import check_repeat_filer
        repeat = check_repeat_filer(entity.ticker, filing.filing_date, all_filings_lookup)

        rows.append({
            "filing_id": filing.id,
            "ticker": entity.ticker,
            "filing_date": filing.filing_date,
            "layoff_date": filing.layoff_date,
            "employees_affected": filing.employees_affected,
            "total_employees": total_employees,
            "sector": entity.sector,
            "market_cap_bucket": entity.market_cap_bucket,
            "repeat_filer": repeat,
            "signal_date": filing.filing_date,
        })

    if not rows:
        logger.warning("No signals to backtest")
        db.close()
        return

    signals_df = pd.DataFrame(rows)
    signals_df = score_signals(signals_df)

    logger.info(f"Scored {len(signals_df)} signals, mean score: {signals_df['composite_score'].mean():.3f}")

    # Store signals to DB
    def _safe_int(v):
        """Convert to int or None if nan/None."""
        if v is None:
            return None
        try:
            if isinstance(v, float) and math.isnan(v):
                return None
            if isinstance(v, (np.floating, np.integer)):
                v = float(v)
            if math.isnan(v):
                return None
            return int(v)
        except (TypeError, ValueError):
            return None

    def _safe_float(v):
        """Convert to float or None if nan/None."""
        if v is None:
            return None
        try:
            f = float(v)
            if math.isnan(f):
                return None
            return f
        except (TypeError, ValueError):
            return None

    for _, sig in signals_df.iterrows():
        existing = db.query(Signal).filter(Signal.filing_id == int(sig["filing_id"])).first()
        if existing:
            continue

        signal = Signal(
            filing_id=int(sig["filing_id"]),
            ticker=sig["ticker"],
            signal_date=sig["signal_date"],
            employees_affected=_safe_int(sig.get("employees_affected")),
            employees_pct=_safe_float(sig.get("employees_pct")),
            filing_lead_days=_safe_int(sig.get("filing_lead_days")),
            repeat_filer=bool(sig.get("repeat_filer", False)),
            sector=sig.get("sector") if pd.notna(sig.get("sector")) else None,
            market_cap_bucket=sig.get("market_cap_bucket") if pd.notna(sig.get("market_cap_bucket")) else None,
            composite_score=float(sig["composite_score"]),
        )
        db.add(signal)

    db.commit()

    # Load prices for backtest
    tickers_needed = set(signals_df["ticker"].unique())
    prices_dict = {}
    for ticker in tickers_needed:
        price_rows = db.query(PriceData).filter(PriceData.ticker == ticker).all()
        if price_rows:
            prices_dict[ticker] = pd.DataFrame([
                {"date": p.date, "open": p.open, "close": p.close}
                for p in price_rows
            ])

    # Run backtest
    config = BacktestConfig(
        hold_days=settings.BACKTEST_DEFAULT_HOLD_DAYS,
        max_positions=settings.BACKTEST_MAX_POSITIONS,
    )

    logger.info(f"Running backtest: hold={config.hold_days}d, max_pos={config.max_positions}")
    result = run_backtest(signals_df, prices_dict, config)
    metrics = compute_metrics(result.trades, result.equity_curve)

    # Store backtest results
    bt_run = BacktestRun(
        run_name="WARN Short Signal v1",
        start_date=signals_df["signal_date"].min(),
        end_date=signals_df["signal_date"].max(),
        config=json.dumps({
            "hold_days": config.hold_days,
            "max_positions": config.max_positions,
            "min_score": config.min_score,
            "transaction_cost_bps": config.transaction_cost_bps,
        }),
        sharpe_ratio=metrics.get("sharpe_ratio"),
        max_drawdown=metrics.get("max_drawdown"),
        total_return=metrics.get("total_return"),
        win_rate=metrics.get("win_rate"),
        n_trades=metrics.get("n_trades"),
    )
    db.add(bt_run)
    db.commit()

    # Store trades
    for trade in result.trades:
        bt_trade = BacktestTrade(
            run_id=bt_run.id,
            filing_id=trade.filing_id,
            ticker=trade.ticker,
            entry_date=trade.entry_date,
            exit_date=trade.exit_date,
            entry_price=trade.entry_price,
            exit_price=trade.exit_price,
            return_pct=trade.return_pct,
            hold_days=trade.hold_days,
        )
        db.add(bt_trade)

    db.commit()
    db.close()

    # Print results
    print("\n" + "=" * 60)
    print("WARN SIGNAL BACKTEST RESULTS")
    print("=" * 60)
    print(f"Sharpe Ratio:     {metrics.get('sharpe_ratio', 'N/A')}")
    print(f"Max Drawdown:     {metrics.get('max_drawdown', 'N/A')}")
    print(f"Total Return:     {metrics.get('total_return', 'N/A')}")
    print(f"Win Rate:         {metrics.get('win_rate', 'N/A')}")
    print(f"Number of Trades: {metrics.get('n_trades', 'N/A')}")
    print(f"Avg Return/Trade: {metrics.get('avg_return', 'N/A')}")
    print("=" * 60)


if __name__ == "__main__":
    run()
