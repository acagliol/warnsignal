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

    # Load prices for backtest (include volume for short-selling filters)
    tickers_needed = set(signals_df["ticker"].unique())
    prices_dict = {}
    for ticker in tickers_needed:
        price_rows = db.query(PriceData).filter(PriceData.ticker == ticker).all()
        if price_rows:
            prices_dict[ticker] = pd.DataFrame([
                {
                    "date": p.date,
                    "open": p.open,
                    "close": p.close,
                    "volume": p.volume,
                }
                for p in price_rows
            ])

    # Helper to store a backtest run in the DB
    def _store_run(run_name, config_obj, result_obj, metrics_obj):
        bt_run = BacktestRun(
            run_name=run_name,
            start_date=signals_df["signal_date"].min(),
            end_date=signals_df["signal_date"].max(),
            config=json.dumps({
                "hold_days": config_obj.hold_days,
                "max_positions": config_obj.max_positions,
                "min_score": config_obj.min_score,
                "transaction_cost_bps": config_obj.transaction_cost_bps,
                "cap_filter": config_obj.cap_filter,
                "use_borrow_costs": config_obj.use_borrow_costs,
                "use_variable_costs": config_obj.use_variable_costs,
                "stop_loss_pct": config_obj.stop_loss_pct,
                "publication_lag_days": config_obj.publication_lag_days,
                "min_price": config_obj.min_price,
                "min_avg_volume": config_obj.min_avg_volume,
            }),
            sharpe_ratio=metrics_obj.get("sharpe_ratio"),
            max_drawdown=metrics_obj.get("max_drawdown"),
            total_return=metrics_obj.get("total_return"),
            win_rate=metrics_obj.get("win_rate"),
            n_trades=metrics_obj.get("n_trades"),
        )
        db.add(bt_run)
        db.commit()

        for trade in result_obj.trades:
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

    # ----------------------------------------------------------------
    # Run 1: Full Sample Short (all signals, no frictions)
    # ----------------------------------------------------------------
    config1 = BacktestConfig(
        hold_days=settings.BACKTEST_DEFAULT_HOLD_DAYS,
        max_positions=settings.BACKTEST_MAX_POSITIONS,
    )

    logger.info(f"Running backtest [Full Sample Short]: hold={config1.hold_days}d, max_pos={config1.max_positions}")
    result1 = run_backtest(signals_df, prices_dict, config1)
    metrics1 = compute_metrics(result1.trades, result1.equity_curve)
    _store_run("WARN Short Signal v1 -- Full Sample Short", config1, result1, metrics1)

    # ----------------------------------------------------------------
    # Run 2: Micro+Small Cap Short (filtered, no frictions)
    # ----------------------------------------------------------------
    config2 = BacktestConfig(
        hold_days=settings.BACKTEST_DEFAULT_HOLD_DAYS,
        max_positions=settings.BACKTEST_MAX_POSITIONS,
        cap_filter=["micro", "small"],
    )

    logger.info(f"Running backtest [Micro+Small Cap Short]: hold={config2.hold_days}d, max_pos={config2.max_positions}")
    result2 = run_backtest(signals_df, prices_dict, config2)
    metrics2 = compute_metrics(result2.trades, result2.equity_curve)
    _store_run("WARN Short Signal v1 -- Micro+Small Cap Short", config2, result2, metrics2)

    # ----------------------------------------------------------------
    # Run 3: Micro+Small Cap + Frictions (borrow costs + variable
    #         transaction costs + 30% stop-loss)
    # ----------------------------------------------------------------
    config3 = BacktestConfig(
        hold_days=settings.BACKTEST_DEFAULT_HOLD_DAYS,
        max_positions=settings.BACKTEST_MAX_POSITIONS,
        cap_filter=["micro", "small"],
        use_borrow_costs=True,
        use_variable_costs=True,
        stop_loss_pct=0.30,
    )

    logger.info(
        f"Running backtest [Micro+Small Cap + Frictions]: hold={config3.hold_days}d, "
        f"max_pos={config3.max_positions}, stop_loss=30%, borrow+variable costs"
    )
    result3 = run_backtest(signals_df, prices_dict, config3)
    metrics3 = compute_metrics(result3.trades, result3.equity_curve)
    _store_run("WARN Short Signal v1 -- Micro+Small Cap + Frictions", config3, result3, metrics3)

    # ----------------------------------------------------------------
    # Run 4: Micro+Small Cap + Publication Lag (3-day lag)
    # ----------------------------------------------------------------
    config4 = BacktestConfig(
        hold_days=settings.BACKTEST_DEFAULT_HOLD_DAYS,
        max_positions=settings.BACKTEST_MAX_POSITIONS,
        cap_filter=["micro", "small"],
        publication_lag_days=3,
    )

    logger.info(
        f"Running backtest [Micro+Small Cap + Publication Lag]: hold={config4.hold_days}d, "
        f"max_pos={config4.max_positions}, pub_lag=3d"
    )
    result4 = run_backtest(signals_df, prices_dict, config4)
    metrics4 = compute_metrics(result4.trades, result4.equity_curve)
    _store_run("WARN Short Signal v1 -- Micro+Small Cap + Publication Lag", config4, result4, metrics4)

    db.close()

    # Print results
    def _print_results(label, metrics):
        print(f"\n{'=' * 60}")
        print(f"  {label}")
        print(f"{'=' * 60}")
        print(f"  Sharpe Ratio:     {metrics.get('sharpe_ratio', 'N/A')}")
        print(f"  Max Drawdown:     {metrics.get('max_drawdown', 'N/A')}")
        print(f"  Total Return:     {metrics.get('total_return', 'N/A')}")
        print(f"  Win Rate:         {metrics.get('win_rate', 'N/A')}")
        print(f"  Number of Trades: {metrics.get('n_trades', 'N/A')}")
        print(f"  Avg Return/Trade: {metrics.get('avg_return', 'N/A')}")
        print(f"{'=' * 60}")

    _print_results("FULL SAMPLE SHORT (all signals, no frictions)", metrics1)
    _print_results("MICRO+SMALL CAP SHORT (filtered, no frictions)", metrics2)
    _print_results("MICRO+SMALL CAP + FRICTIONS (borrow + variable costs + 30% stop)", metrics3)
    _print_results("MICRO+SMALL CAP + PUBLICATION LAG (3-day lag)", metrics4)


if __name__ == "__main__":
    run()
