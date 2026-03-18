import time
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class BaseScraper(ABC):
    """Abstract base class for WARN Act scrapers with retry and rate limiting."""

    STATE: str = ""
    BASE_URL: str = ""

    def __init__(self, delay_seconds: float = 2.0):
        self.delay = delay_seconds
        self.logger = logging.getLogger(f"scraper.{self.STATE}")
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })
        return session

    def _rate_limit(self):
        time.sleep(self.delay)

    def _get(self, url: str, **kwargs) -> requests.Response:
        self.logger.info(f"Fetching {url}")
        resp = self.session.get(url, timeout=30, **kwargs)
        resp.raise_for_status()
        self._rate_limit()
        return resp

    @abstractmethod
    def scrape(self) -> List[Dict[str, Any]]:
        """Scrape WARN filings and return list of standardized dicts.

        Each dict must have:
            company_name: str
            filing_date: date
            layoff_date: Optional[date]
            employees_affected: Optional[int]
            location: Optional[str]
            source_url: str
        """
        pass

    @staticmethod
    def parse_date(val: Any) -> Optional[date]:
        """Try to parse a date from various formats."""
        if val is None or (isinstance(val, str) and val.strip() == ""):
            return None
        import pandas as pd
        try:
            parsed = pd.to_datetime(val, format="mixed", dayfirst=False)
            if pd.isna(parsed):
                return None
            return parsed.date()
        except Exception:
            return None

    @staticmethod
    def parse_int(val: Any) -> Optional[int]:
        """Try to parse an integer from various formats."""
        if val is None:
            return None
        if isinstance(val, (int, float)):
            return int(val) if not (isinstance(val, float) and val != val) else None
        try:
            cleaned = str(val).replace(",", "").strip()
            if cleaned == "" or cleaned.lower() == "nan":
                return None
            return int(float(cleaned))
        except (ValueError, TypeError):
            return None
