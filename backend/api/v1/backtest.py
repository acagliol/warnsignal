import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import get_db
from models import BacktestRun, BacktestTrade, EventStudyResult, EntityMatch, Signal
from schemas.backtest_schema import (
    BacktestConfig,
    BacktestResultResponse,
    BacktestTradeResponse,
    BacktestStatsResponse,
    EquityCurvePoint,
)
from services.event_study.statistics import compute_car_statistics, compute_breakdown
import pandas as pd

router = APIRouter(prefix="/api/v1/backtest", tags=["backtest"])


@router.get("/results", response_model=Optional[BacktestResultResponse])
def get_latest_results(db: Session = Depends(get_db)):
    bt_run = db.query(BacktestRun).order_by(BacktestRun.created_at.desc()).first()
    if not bt_run:
        raise HTTPException(status_code=404, detail="No backtest results found")

    trades = db.query(BacktestTrade).filter(BacktestTrade.run_id == bt_run.id).all()

    return BacktestResultResponse(
        run_id=bt_run.id,
        run_name=bt_run.run_name,
        sharpe_ratio=bt_run.sharpe_ratio,
        max_drawdown=bt_run.max_drawdown,
        total_return=bt_run.total_return,
        win_rate=bt_run.win_rate,
        n_trades=bt_run.n_trades,
        trades=[
            BacktestTradeResponse(
                ticker=t.ticker,
                entry_date=t.entry_date,
                exit_date=t.exit_date,
                entry_price=t.entry_price,
                exit_price=t.exit_price,
                return_pct=t.return_pct,
                hold_days=t.hold_days,
            )
            for t in trades
        ],
    )


@router.get("/results/{run_id}", response_model=BacktestResultResponse)
def get_results(run_id: int, db: Session = Depends(get_db)):
    bt_run = db.query(BacktestRun).filter(BacktestRun.id == run_id).first()
    if not bt_run:
        raise HTTPException(status_code=404, detail="Backtest run not found")

    trades = db.query(BacktestTrade).filter(BacktestTrade.run_id == bt_run.id).all()

    return BacktestResultResponse(
        run_id=bt_run.id,
        run_name=bt_run.run_name,
        sharpe_ratio=bt_run.sharpe_ratio,
        max_drawdown=bt_run.max_drawdown,
        total_return=bt_run.total_return,
        win_rate=bt_run.win_rate,
        n_trades=bt_run.n_trades,
        trades=[
            BacktestTradeResponse(
                ticker=t.ticker,
                entry_date=t.entry_date,
                exit_date=t.exit_date,
                entry_price=t.entry_price,
                exit_price=t.exit_price,
                return_pct=t.return_pct,
                hold_days=t.hold_days,
            )
            for t in trades
        ],
    )


@router.get("/stats", response_model=BacktestStatsResponse)
def get_stats(db: Session = Depends(get_db)):
    # Get all event study results with entity data
    results = (
        db.query(EventStudyResult, EntityMatch)
        .join(EntityMatch, EventStudyResult.filing_id == EntityMatch.filing_id)
        .all()
    )

    if not results:
        return BacktestStatsResponse(n_events=0)

    rows = []
    for es, entity in results:
        rows.append({
            "car_post30": es.car_post30,
            "sector": entity.sector,
            "market_cap_bucket": entity.market_cap_bucket,
        })

    df = pd.DataFrame(rows)
    car_values = df["car_post30"].dropna().tolist()
    car_stats = compute_car_statistics(car_values)

    sector_breakdown = {}
    if "sector" in df.columns:
        sector_breakdown = compute_breakdown(df, "sector")

    cap_breakdown = {}
    if "market_cap_bucket" in df.columns:
        cap_breakdown = compute_breakdown(df, "market_cap_bucket")

    return BacktestStatsResponse(
        mean_car_post30=car_stats.get("mean"),
        median_car_post30=car_stats.get("median"),
        t_stat=car_stats.get("t_stat"),
        p_value=car_stats.get("p_value"),
        pct_negative=car_stats.get("pct_negative"),
        n_events=car_stats.get("n_events", 0),
        sector_breakdown=sector_breakdown,
        cap_breakdown=cap_breakdown,
    )
