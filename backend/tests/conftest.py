"""Test fixtures for WARNSignal."""

import sys
import os
from datetime import date, timedelta

import pytest
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database import Base
from models import WarnFiling, EntityMatch, PriceData


@pytest.fixture
def db_engine():
    """Create an in-memory SQLite engine for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(bind=engine)
    return engine


@pytest.fixture
def db_session(db_engine):
    """Create a database session for testing."""
    Session = sessionmaker(bind=db_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_filings(db_session):
    """Insert sample WARN filings."""
    filings = [
        WarnFiling(
            state="NY",
            company_name_raw="BED BATH & BEYOND INC",
            filing_date=date(2023, 1, 5),
            layoff_date=date(2023, 2, 1),
            employees_affected=500,
            location="New York",
        ),
        WarnFiling(
            state="CA",
            company_name_raw="RITE AID CORPORATION",
            filing_date=date(2023, 6, 15),
            layoff_date=date(2023, 7, 15),
            employees_affected=200,
            location="Los Angeles",
        ),
        WarnFiling(
            state="TX",
            company_name_raw="PARTY CITY HOLDCO INC",
            filing_date=date(2022, 12, 1),
            layoff_date=date(2023, 1, 15),
            employees_affected=300,
            location="Houston",
        ),
        WarnFiling(
            state="NY",
            company_name_raw="REVLON INC",
            filing_date=date(2022, 5, 1),
            layoff_date=date(2022, 6, 15),
            employees_affected=150,
            location="New York",
        ),
        WarnFiling(
            state="CA",
            company_name_raw="JOE'S LOCAL PIZZA LLC",
            filing_date=date(2023, 3, 1),
            layoff_date=date(2023, 4, 1),
            employees_affected=15,
            location="San Francisco",
        ),
    ]

    for f in filings:
        db_session.add(f)
    db_session.commit()
    return filings


@pytest.fixture
def synthetic_prices():
    """Generate synthetic price data with a known abnormal return at event date.

    Creates:
    - Stock with a -5% abnormal return at day 0 (event date)
    - Benchmark (market) with normal returns
    - 400 trading days of data (estimation + event window)
    """
    np.random.seed(42)
    n_days = 400
    event_day = 300  # Day index for the event

    # Market returns: normal(0.0003, 0.01)
    market_returns = np.random.normal(0.0003, 0.01, n_days)

    # Stock returns: correlated with market (beta=1.2, alpha=0.0001)
    alpha = 0.0001
    beta = 1.2
    noise = np.random.normal(0, 0.008, n_days)
    stock_returns = alpha + beta * market_returns + noise

    # Inject -5% abnormal return at event date
    stock_returns[event_day] -= 0.05

    # Build price series
    start_date = date(2022, 1, 3)
    dates = [start_date + timedelta(days=i) for i in range(n_days)]

    stock_prices = [100.0]
    market_prices = [100.0]
    for i in range(n_days):
        stock_prices.append(stock_prices[-1] * (1 + stock_returns[i]))
        market_prices.append(market_prices[-1] * (1 + market_returns[i]))

    stock_df = pd.DataFrame({
        "date": dates + [dates[-1] + timedelta(days=1)],
        "close": stock_prices,
        "open": stock_prices,
    })

    market_df = pd.DataFrame({
        "date": dates + [dates[-1] + timedelta(days=1)],
        "close": market_prices,
        "open": market_prices,
    })

    event_date = dates[event_day]

    return {
        "stock_df": stock_df,
        "market_df": market_df,
        "event_date": event_date,
        "event_day_index": event_day,
        "injected_abnormal_return": -0.05,
    }
