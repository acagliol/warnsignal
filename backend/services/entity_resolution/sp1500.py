"""S&P 1500 constituent list for ticker/name fuzzy matching.

Loads a static CSV of S&P 1500 constituents with columns:
ticker, company_name, sector, market_cap

If the CSV doesn't exist, fetches from SEC company_tickers.json as a baseline.
"""

import os
import re
import json
import logging
from typing import Dict, Tuple, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# Suffixes to strip during normalization
STRIP_SUFFIXES = [
    r"\bINC\.?$", r"\bCORP\.?$", r"\bCORPORATION$", r"\bCO\.?$",
    r"\bLLC$", r"\bLTD\.?$", r"\bLTD$", r"\bL\.?P\.?$", r"\bPLC$",
    r"\bHOLDINGS?$", r"\bHOLDCO$", r"\bGROUP$", r"\bENTERPRISES?$",
    r"\bINTERNATIONAL$", r"\bINTL\.?$", r"\bCOMPANY$", r"\bSERVICES?$",
    r"\bSOLUTIONS?$", r"\bTECHNOLOGIE?S?$", r"\bINDUSTRIE?S?$",
    r"\b& CO\.?$", r"\bN\.?A\.?$",
]

# GICS sector mapping for common ETF benchmarks
SECTOR_ETF = {
    "Technology": "XLK",
    "Information Technology": "XLK",
    "Health Care": "XLV",
    "Healthcare": "XLV",
    "Financials": "XLF",
    "Financial": "XLF",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Industrials": "XLI",
    "Industrial": "XLI",
    "Materials": "XLB",
    "Basic Materials": "XLB",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
    "Communication": "XLC",
    "Telecommunications": "XLC",
}


def normalize_company_name(name: str) -> str:
    """Normalize company name for matching."""
    name = name.upper().strip()
    # Remove common punctuation
    name = re.sub(r"[,.'\"!@#$%^&*()\[\]{}]", "", name)
    # Strip suffixes iteratively
    for pattern in STRIP_SUFFIXES:
        name = re.sub(pattern, "", name).strip()
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def get_market_cap_bucket(market_cap: Optional[float]) -> Optional[str]:
    """Categorize market cap into buckets."""
    if market_cap is None:
        return None
    if market_cap > 200e9:
        return "mega"
    elif market_cap > 10e9:
        return "large"
    elif market_cap > 2e9:
        return "mid"
    elif market_cap > 300e6:
        return "small"
    else:
        return "micro"


class SP1500Index:
    """Loads and indexes S&P 1500 constituents for fuzzy matching."""

    def __init__(self, csv_path: Optional[str] = None):
        if csv_path is None:
            csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "sp1500_constituents.csv")

        self.csv_path = csv_path
        self._names: Dict[str, Tuple[str, str, Optional[float]]] = {}  # normalized_name -> (ticker, sector, mcap)
        self._tickers: Dict[str, str] = {}  # ticker -> company_name
        self._load()

    def _load(self):
        """Load constituent data from CSV or SEC fallback."""
        if os.path.exists(self.csv_path):
            logger.info(f"Loading S&P 1500 data from {self.csv_path}")
            df = pd.read_csv(self.csv_path)
            for _, row in df.iterrows():
                ticker = str(row.get("ticker", "")).strip().upper()
                name = str(row.get("company_name", row.get("name", ""))).strip()
                sector = str(row.get("sector", "")).strip() if pd.notna(row.get("sector")) else None
                mcap = float(row["market_cap"]) if pd.notna(row.get("market_cap")) else None

                if ticker and name:
                    norm = normalize_company_name(name)
                    self._names[norm] = (ticker, sector, mcap)
                    self._tickers[ticker] = name
        else:
            logger.info("No SP1500 CSV found, loading from SEC company_tickers.json")
            self._load_from_sec()

    def _load_from_sec(self):
        """Fallback: load tickers from SEC's company_tickers.json."""
        try:
            resp = requests.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers={"User-Agent": "WARNSignal research@warnsignal.dev"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            for entry in data.values():
                ticker = entry.get("ticker", "").strip().upper()
                name = entry.get("title", "").strip()
                if ticker and name:
                    norm = normalize_company_name(name)
                    self._names[norm] = (ticker, None, None)
                    self._tickers[ticker] = name

            logger.info(f"Loaded {len(self._names)} companies from SEC")

            # Save as CSV for next time
            os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
            rows = [
                {"ticker": v[0], "company_name": k, "sector": v[1] or "", "market_cap": v[2] or ""}
                for k, v in self._names.items()
            ]
            pd.DataFrame(rows).to_csv(self.csv_path, index=False)

        except Exception as e:
            logger.error(f"Failed to load from SEC: {e}")

    @property
    def names(self) -> list:
        return list(self._names.keys())

    def lookup(self, normalized_name: str) -> Optional[Tuple[str, Optional[str], Optional[float]]]:
        """Look up by exact normalized name. Returns (ticker, sector, market_cap) or None."""
        return self._names.get(normalized_name)

    def ticker_exists(self, ticker: str) -> bool:
        return ticker.upper() in self._tickers

    def get_sector_etf(self, sector: Optional[str]) -> str:
        """Get the sector ETF ticker for benchmarking. Defaults to SPY."""
        if sector is None:
            return "SPY"
        return SECTOR_ETF.get(sector, "SPY")
