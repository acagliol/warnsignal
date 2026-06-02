"""Entity resolution pipeline: match WARN company names to stock tickers.

Pipeline:
0. Subsidiary / DBA map lookup (score 95)
1. Normalize company name (strip suffixes, punctuation, DBA, location, store #)
2. Exact match against S&P 1500 / SEC universe (score 100)
3. Fuzzy match via rapidfuzz (threshold >= 85)
4. SEC EDGAR fallback for low-confidence matches (score 80)
5. OpenFIGI API lookup (score 78)
6. Token-based first-word match (discounted partial)
7. Store result with match_method and confidence score
"""

import re
import logging
from typing import Optional, Dict, Any

from rapidfuzz import fuzz, process

from services.entity_resolution.sp1500 import SP1500Index, normalize_company_name, get_market_cap_bucket
from services.entity_resolution.sec_client import SECClient
from services.entity_resolution.subsidiary_map import lookup_subsidiary
from services.entity_resolution.openfigi_client import OpenFIGIClient

logger = logging.getLogger(__name__)

# ---- Enhanced normalization helpers ----

_LOCATION_TAIL = re.compile(
    r"\s*[-–—]\s*[A-Z][A-Za-z\s,]+(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\s*$",
)
_STORE_NUMBER = re.compile(
    r"\s*(?:#|STORE\s*#?|PLANT\s*#?|FACILITY\s*#?|UNIT\s*#?|LOCATION\s*#?|SITE\s*#?)\s*\d+",
    re.IGNORECASE,
)
_DBA = re.compile(r"\s*(?:D/?B/?A|DOING BUSINESS AS)\s+", re.IGNORECASE)


def _enhanced_normalize(name: str) -> str:
    """Extended normalization on top of sp1500.normalize_company_name.

    Strips location suffixes, store/plant numbers, and DBA prefixes before
    delegating to the standard normalizer.
    """
    name = name.strip()
    # Uppercase for regex matching
    upper = name.upper()

    # Strip location suffix ("- Springfield, IL")
    upper = _LOCATION_TAIL.sub("", upper)

    # Strip store / plant / facility numbers
    upper = _STORE_NUMBER.sub("", upper)

    # Handle "DBA" — keep only the DBA part for matching
    parts = _DBA.split(upper, maxsplit=1)
    if len(parts) == 2:
        upper = parts[1]

    # Delegate to the existing normalizer (strips corporate suffixes, punctuation, etc.)
    return normalize_company_name(upper)


class EntityResolver:
    """Resolves raw company names to stock tickers."""

    def __init__(
        self,
        match_threshold: int = 85,
        sec_user_agent: str = "WARNSignal research@warnsignal.dev",
        openfigi_api_key: Optional[str] = None,
    ):
        self.threshold = match_threshold
        self.sp1500 = SP1500Index()
        self.sec_client = SECClient(user_agent=sec_user_agent)
        self.openfigi_client = OpenFIGIClient(api_key=openfigi_api_key)

    def _enrich_from_sp1500(self, result: Dict[str, Any], ticker: str) -> None:
        """Fill in sector / market-cap fields from SP1500 if available."""
        sp_info = self.sp1500.lookup_by_ticker(ticker)
        if sp_info:
            result["sector"] = sp_info[1]
            result["market_cap_bucket"] = get_market_cap_bucket(sp_info[2])

    def resolve(self, company_name_raw: str) -> Dict[str, Any]:
        """Resolve a raw company name to a ticker.

        Returns dict with:
            ticker: str or None
            company_name_matched: str
            match_method: str ("subsidiary_map", "exact", "rapidfuzz",
                               "sec_edgar", "openfigi", "token_match", or None)
            match_score: float (0-100)
            cik: str or None
            sector: str or None
            market_cap_bucket: str or None
        """
        result: Dict[str, Any] = {
            "ticker": None,
            "company_name_matched": None,
            "match_method": None,
            "match_score": 0.0,
            "cik": None,
            "sector": None,
            "market_cap_bucket": None,
        }

        # ---- Step 0: Subsidiary / DBA map lookup ----
        sub_ticker = lookup_subsidiary(company_name_raw)
        if sub_ticker:
            result.update({
                "ticker": sub_ticker,
                "company_name_matched": company_name_raw,
                "match_method": "subsidiary_map",
                "match_score": 95.0,
            })
            self._enrich_from_sp1500(result, sub_ticker)
            logger.info(f"Subsidiary map match: '{company_name_raw}' -> {sub_ticker}")
            return result

        # Enhanced normalization (strips DBA, location tails, store numbers)
        normalized = _enhanced_normalize(company_name_raw)

        # ---- Step 1: Exact match ----
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

        # ---- Step 2: Fuzzy match via rapidfuzz ----
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

        # ---- Step 3: SEC EDGAR fallback ----
        sec_result = self.sec_client.search_company(company_name_raw)
        if sec_result:
            ticker, cik = sec_result
            sp_info = self.sp1500.lookup_by_ticker(ticker)
            sector = sp_info[1] if sp_info else None
            mcap = sp_info[2] if sp_info else None

            result.update({
                "ticker": ticker,
                "company_name_matched": company_name_raw,
                "match_method": "sec_edgar",
                "match_score": 80.0,
                "cik": cik,
                "sector": sector,
                "market_cap_bucket": get_market_cap_bucket(mcap) if mcap else None,
            })
            logger.info(f"SEC EDGAR match: '{company_name_raw}' -> {ticker} (CIK={cik})")
            return result

        # ---- Step 4: OpenFIGI API lookup ----
        figi_result = self.openfigi_client.lookup(company_name_raw)
        if figi_result:
            ticker, figi_id = figi_result
            result.update({
                "ticker": ticker,
                "company_name_matched": company_name_raw,
                "match_method": "openfigi",
                "match_score": 78.0,
            })
            self._enrich_from_sp1500(result, ticker)
            logger.info(f"OpenFIGI match: '{company_name_raw}' -> {ticker} (FIGI={figi_id})")
            return result

        # ---- Step 5: Token-based first-word match ----
        first_word = normalized.split()[0] if normalized else ""
        if len(first_word) >= 4 and self.sp1500.names:
            token_match = process.extractOne(
                first_word,
                self.sp1500.names,
                scorer=fuzz.partial_ratio,
            )
            if token_match and token_match[1] >= 90:
                matched_name = token_match[0]
                lookup = self.sp1500.lookup(matched_name)
                if lookup:
                    ticker, sector, mcap = lookup
                    result.update({
                        "ticker": ticker,
                        "company_name_matched": matched_name,
                        "match_method": "token_match",
                        "match_score": token_match[1] * 0.85,  # Discount for partial match
                        "sector": sector,
                        "market_cap_bucket": get_market_cap_bucket(mcap),
                    })
                    logger.info(f"Token match: '{company_name_raw}' -> {ticker} (score={token_match[1]:.1f})")
                    return result

        # No match found
        logger.debug(f"No match found for: '{company_name_raw}'")
        return result
