"""Entity resolution pipeline: match WARN company names to stock tickers.

Pipeline:
1. Normalize company name (strip suffixes, punctuation, uppercase)
2. Exact match against S&P 1500 / SEC universe
3. Fuzzy match via rapidfuzz (threshold >= 85)
4. SEC EDGAR fallback for low-confidence matches
5. Store result with match_method and confidence score
"""

import logging
from typing import Optional, Dict, Any

from rapidfuzz import fuzz, process

from services.entity_resolution.sp1500 import SP1500Index, normalize_company_name, get_market_cap_bucket
from services.entity_resolution.sec_client import SECClient

logger = logging.getLogger(__name__)


class EntityResolver:
    """Resolves raw company names to stock tickers."""

    def __init__(self, match_threshold: int = 85, sec_user_agent: str = "WARNSignal research@warnsignal.dev"):
        self.threshold = match_threshold
        self.sp1500 = SP1500Index()
        self.sec_client = SECClient(user_agent=sec_user_agent)

    def resolve(self, company_name_raw: str) -> Dict[str, Any]:
        """Resolve a raw company name to a ticker.

        Returns dict with:
            ticker: str or None
            company_name_matched: str
            match_method: str ("exact", "rapidfuzz", "sec_edgar", None)
            match_score: float (0-100)
            cik: str or None
            sector: str or None
            market_cap_bucket: str or None
        """
        normalized = normalize_company_name(company_name_raw)
        result = {
            "ticker": None,
            "company_name_matched": None,
            "match_method": None,
            "match_score": 0.0,
            "cik": None,
            "sector": None,
            "market_cap_bucket": None,
        }

        # Step 1: Exact match
        exact = self.sp1500.lookup(normalized)
        if exact:
            ticker, sector, mcap = exact
            result.update({
                "ticker": ticker,
                "company_name_matched": normalized,
                "match_method": "exact",
                "match_score": 100.0,
                "sector": sector,
                "market_cap_bucket": get_market_cap_bucket(mcap),
            })
            logger.info(f"Exact match: '{company_name_raw}' -> {ticker}")
            return result

        # Step 2: Fuzzy match via rapidfuzz
        if self.sp1500.names:
            match = process.extractOne(
                normalized,
                self.sp1500.names,
                scorer=fuzz.token_sort_ratio,
            )

            if match and match[1] >= self.threshold:
                matched_name = match[0]
                score = match[1]
                lookup = self.sp1500.lookup(matched_name)
                if lookup:
                    ticker, sector, mcap = lookup
                    result.update({
                        "ticker": ticker,
                        "company_name_matched": matched_name,
                        "match_method": "rapidfuzz",
                        "match_score": score,
                        "sector": sector,
                        "market_cap_bucket": get_market_cap_bucket(mcap),
                    })
                    logger.info(f"Fuzzy match: '{company_name_raw}' -> {ticker} (score={score:.1f})")
                    return result

        # Step 3: SEC EDGAR fallback
        sec_result = self.sec_client.search_company(company_name_raw)
        if sec_result:
            ticker, cik = sec_result
            if self.sp1500.ticker_exists(ticker):
                result.update({
                    "ticker": ticker,
                    "company_name_matched": company_name_raw,
                    "match_method": "sec_edgar",
                    "match_score": 75.0,  # Lower confidence for EDGAR-only matches
                    "cik": cik,
                })
                logger.info(f"SEC EDGAR match: '{company_name_raw}' -> {ticker} (CIK={cik})")
                return result

        # No match found
        logger.debug(f"No match found for: '{company_name_raw}'")
        return result
