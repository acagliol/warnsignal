"""CLI: Generate the research report with charts."""

import sys
import os
import json
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from database import engine, SessionLocal, Base
from models import EventStudyResult, EntityMatch, BacktestRun, BacktestTrade
from services.event_study.statistics import compute_full_statistics
from services.backtest.metrics import compute_metrics
from services.report.generator import generate_report
from services.report.research_memo import generate_research_memo

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    # Load all event study results
    results = (
        db.query(EventStudyResult, EntityMatch)
        .join(EntityMatch, EventStudyResult.filing_id == EntityMatch.filing_id)
        .all()
    )

    if not results:
        logger.warning("No event study results found")
        db.close()
        return

    # Build DataFrame
    rows = []
    car_timeseries_list = []
    for es, entity in results:
        rows.append({
            "car_pre30": es.car_pre30,
            "car_post30": es.car_post30,
            "car_post60": es.car_post60,
            "car_post90": es.car_post90,
            "sector": entity.sector,
            "market_cap_bucket": entity.market_cap_bucket,
        })
        if es.car_timeseries:
            try:
                car_timeseries_list.append(json.loads(es.car_timeseries))
            except json.JSONDecodeError:
                pass

    results_df = pd.DataFrame(rows)

    # Compute statistics
    stats = compute_full_statistics(results_df, car_timeseries_list)

    # Load latest backtest results
    bt_run = db.query(BacktestRun).order_by(BacktestRun.created_at.desc()).first()
    backtest_metrics = {}
    equity_curve = []

    if bt_run:
        backtest_metrics = {
            "sharpe_ratio": bt_run.sharpe_ratio,
            "max_drawdown": bt_run.max_drawdown,
            "total_return": bt_run.total_return,
            "win_rate": bt_run.win_rate,
            "n_trades": bt_run.n_trades,
        }

    db.close()

    # Generate report
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "output")
    report_path = generate_report(
        stats=stats,
        backtest_metrics=backtest_metrics,
        equity_curve=equity_curve,
        car_timeseries_list=car_timeseries_list,
        output_dir=output_dir,
    )

    # Generate one-page PDF research memo
    n_events = stats.get("car_post30", {}).get("n_events", 0)
    memo_path = generate_research_memo(
        stats=stats,
        backtest_metrics=backtest_metrics,
        n_events_total=n_events,
        output_dir=output_dir,
    )

    print(f"\nReport generated: {report_path}")
    print(f"Research memo: {memo_path}")


if __name__ == "__main__":
    run()
