"""Price data loader using yfinance.

Fetches historical OHLCV data for stocks and sector ETF benchmarks.
Handles split/dividend adjustment automatically via yfinance.
"""

import logging
from datetime import date, timedelta
from typing import Optional, List, Dict

import pandas as pd
import yfinance as yf

from services.entity_resolution.sp1500 import SECTOR_ETF

logger = logging.getLogger(__name__)

# All sector ETFs we might need as benchmarks
ALL_SECTOR_ETFS = list(set(SECTOR_ETF.values())) + ["SPY"]


def fetch_prices(
    ticker: str,
    start_date: date,
    end_date: date,
) -> Optional[pd.DataFrame]:
    """Fetch adjusted OHLCV data for a single ticker.

    Returns DataFrame with columns: date, open, high, low, close, volume
    or None if fetch fails.
    """
    try:
        # Add buffer for estimation window
        yf_ticker = yf.Ticker(ticker)
        df = yf_ticker.history(
            start=start_date.isoformat(),
            end=(end_date + timedelta(days=1)).isoformat(),
            auto_adjust=True,  # Adjust for splits and dividends
        )

        if df.empty:
            logger.warning(f"No price data for {ticker}")
            return None

        df = df.reset_index()
        df = df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })

        # Ensure date is date type (not datetime)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["ticker"] = ticker

        return df[["ticker", "date", "open", "high", "low", "close", "volume"]]

    except Exception as e:
        logger.error(f"Failed to fetch prices for {ticker}: {e}")
        return None


def fetch_prices_batch(
    tickers: List[str],
    start_date: date,
    end_date: date,
) -> Dict[str, pd.DataFrame]:
    """Fetch prices for multiple tickers at once.

    Returns dict of ticker -> DataFrame.
    """
    results = {}

    # yfinance supports batch downloads
    try:
        ticker_str = " ".join(tickers)
        data = yf.download(
            ticker_str,
            start=start_date.isoformat(),
            end=(end_date + timedelta(days=1)).isoformat(),
            auto_adjust=True,
            group_by="ticker",
            threads=True,
        )

        if data.empty:
            logger.warning("Batch download returned empty data")
            return results

        for ticker in tickers:
            try:
                if len(tickers) == 1:
                    df = data.copy()
                else:
                    df = data[ticker].copy()

                df = df.dropna(subset=["Close"])
                if df.empty:
                    continue

                df = df.reset_index()
                df = df.rename(columns={
                    "Date": "date",
                    "Open": "open",
                    "High": "high",
                    "Low": "low",
                    "Close": "close",
                    "Volume": "volume",
                })
                df["date"] = pd.to_datetime(df["date"]).dt.date
                df["ticker"] = ticker
                results[ticker] = df[["ticker", "date", "open", "high", "low", "close", "volume"]]

            except (KeyError, Exception) as e:
                logger.warning(f"Failed to extract {ticker} from batch: {e}")

    except Exception as e:
        logger.error(f"Batch download failed: {e}")
        # Fallback to individual fetches
        for ticker in tickers:
            df = fetch_prices(ticker, start_date, end_date)
            if df is not None:
                results[ticker] = df

    return results


def get_benchmark_ticker(sector: Optional[str]) -> str:
    """Get the sector ETF ticker for benchmarking."""
    if sector is None:
        return "SPY"
    return SECTOR_ETF.get(sector, "SPY")


def get_company_info(ticker: str) -> Dict:
    """Get company info from yfinance (employees, sector, market cap)."""
    try:
        info = yf.Ticker(ticker).info
        return {
            "full_time_employees": info.get("fullTimeEmployees"),
            "sector": info.get("sector"),
            "market_cap": info.get("marketCap"),
            "industry": info.get("industry"),
        }
    except Exception as e:
        logger.warning(f"Failed to get info for {ticker}: {e}")
        return {}
