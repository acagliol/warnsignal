"""SEC EDGAR API client for entity resolution.

Uses the EDGAR full-text search (EFTS) API and company_tickers.json
to resolve company names to tickers.
"""

import logging
import time
from typing import Optional, Tuple

import requests

logger = logging.getLogger(__name__)

SEC_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_COMPANY_URL = "https://www.sec.gov/cgi-bin/browse-edgar"

# SEC requires a User-Agent with contact info
DEFAULT_UA = "WARNSignal research@warnsignal.dev"


class SECClient:
    """Client for SEC EDGAR lookups."""

    def __init__(self, user_agent: str = DEFAULT_UA, delay: float = 0.5):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        self.delay = delay
        self._cik_to_ticker: Optional[dict] = None

    def _load_cik_ticker_map(self):
        """Load CIK-to-ticker mapping from SEC company_tickers.json."""
        if self._cik_to_ticker is not None:
            return

        try:
            resp = self.session.get(SEC_TICKERS_URL, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            self._cik_to_ticker = {}
            for entry in data.values():
                cik = str(entry.get("cik_str", "")).strip()
                ticker = entry.get("ticker", "").strip().upper()
                if cik and ticker:
                    self._cik_to_ticker[cik] = ticker

            logger.info(f"Loaded {len(self._cik_to_ticker)} CIK-to-ticker mappings from SEC")
            time.sleep(self.delay)

        except Exception as e:
            logger.error(f"Failed to load CIK-ticker map: {e}")
            self._cik_to_ticker = {}

    def search_company(self, company_name: str) -> Optional[Tuple[str, str]]:
        """Search EDGAR EFTS for a company name.

        Returns (ticker, cik) if found, or None.
        """
        self._load_cik_ticker_map()

        try:
            params = {
                "q": f'"{company_name}"',
                "dateRange": "custom",
                "startdt": "2018-01-01",
                "enddt": "2025-12-31",
            }
            resp = self.session.get(SEC_SEARCH_URL, params=params, timeout=15)
            time.sleep(self.delay)

            if resp.status_code != 200:
                # EFTS may not be available; try direct company search
                return self._search_company_direct(company_name)

            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])

            if not hits:
                return self._search_company_direct(company_name)

            # Extract CIK from first result
            for hit in hits[:5]:
                source = hit.get("_source", {})
                entity_name = source.get("entity_name", "")
                cik = str(source.get("entity_id", "")).strip()

                if cik and cik in self._cik_to_ticker:
                    ticker = self._cik_to_ticker[cik]
                    return (ticker, cik)

            return None

        except Exception as e:
            logger.warning(f"EDGAR search failed for '{company_name}': {e}")
            return self._search_company_direct(company_name)

    def _search_company_direct(self, company_name: str) -> Optional[Tuple[str, str]]:
        """Fallback: search via EDGAR company search page."""
        try:
            params = {
                "company": company_name,
                "CIK": "",
                "type": "10-K",
                "dateb": "",
                "owner": "include",
                "count": "5",
                "search_text": "",
                "action": "getcompany",
            }
            resp = self.session.get(SEC_COMPANY_URL, params=params, timeout=15)
            time.sleep(self.delay)

            if resp.status_code != 200:
                return None

            # Parse the HTML response for CIK numbers
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "lxml")

            # Find CIK links in results table
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "CIK=" in href:
                    import re
                    match = re.search(r"CIK=(\d+)", href)
                    if match:
                        cik = match.group(1)
                        if cik in self._cik_to_ticker:
                            return (self._cik_to_ticker[cik], cik)

            return None

        except Exception as e:
            logger.warning(f"Direct EDGAR search failed for '{company_name}': {e}")
            return None
