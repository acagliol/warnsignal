"""OpenFIGI API client for resolving company names to tickers.

Uses the OpenFIGI v3 mapping API to look up FIGI identifiers and tickers
from free-text company name queries. Rate limited to 10 req/min without an API key.
"""

import logging
import time
from typing import Optional, Tuple

import requests

logger = logging.getLogger(__name__)

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# Without an API key the limit is 10 requests per minute.
DEFAULT_RATE_LIMIT = 10
DEFAULT_RATE_WINDOW = 60  # seconds


class OpenFIGIClient:
    """Query the OpenFIGI API to resolve company names to (ticker, figi_id)."""

    def __init__(self, api_key: Optional[str] = None, rate_limit: int = DEFAULT_RATE_LIMIT):
        self.api_key = api_key
        self.rate_limit = rate_limit
        self._request_times: list[float] = []
        self.session = requests.Session()
        if api_key:
            self.session.headers["X-OPENFIGI-APIKEY"] = api_key

    # ------------------------------------------------------------------
    # Rate-limiting
    # ------------------------------------------------------------------

    def _wait_for_rate_limit(self) -> None:
        """Block until we are within the per-minute rate limit."""
        now = time.monotonic()
        # Discard timestamps older than the rate window
        self._request_times = [t for t in self._request_times if now - t < DEFAULT_RATE_WINDOW]

        if len(self._request_times) >= self.rate_limit:
            oldest = self._request_times[0]
            sleep_for = DEFAULT_RATE_WINDOW - (now - oldest) + 0.1
            if sleep_for > 0:
                logger.debug(f"OpenFIGI rate limit reached, sleeping {sleep_for:.1f}s")
                time.sleep(sleep_for)

        self._request_times.append(time.monotonic())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def lookup(self, company_name: str) -> Optional[Tuple[str, str]]:
        """Resolve *company_name* via OpenFIGI.

        Returns
        -------
        (ticker, figi_id) on success, or ``None`` if no match / error.
        """
        if not company_name or not company_name.strip():
            return None

        self._wait_for_rate_limit()

        payload = [{"query": company_name, "exchCode": "US"}]

        try:
            resp = self.session.post(
                OPENFIGI_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )

            # 429 → rate-limited; back off and retry once
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", DEFAULT_RATE_WINDOW))
                logger.warning(f"OpenFIGI 429 rate-limited, retrying after {retry_after}s")
                time.sleep(retry_after)
                resp = self.session.post(
                    OPENFIGI_URL,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=15,
                )

            if resp.status_code != 200:
                logger.warning(f"OpenFIGI returned status {resp.status_code} for '{company_name}'")
                return None

            data = resp.json()
            if not data or not isinstance(data, list):
                return None

            first = data[0]

            # The API wraps results in a "data" key on success
            if "data" not in first:
                # Could be {"warning": "No identifier found."} or similar
                return None

            for item in first["data"]:
                ticker = item.get("ticker")
                figi = item.get("figi")
                if ticker:
                    logger.info(f"OpenFIGI match: '{company_name}' -> {ticker} (FIGI={figi})")
                    return (ticker, figi or "")

            return None

        except requests.exceptions.Timeout:
            logger.warning(f"OpenFIGI request timed out for '{company_name}'")
            return None
        except requests.exceptions.RequestException as exc:
            logger.warning(f"OpenFIGI request failed for '{company_name}': {exc}")
            return None
        except (ValueError, KeyError, IndexError) as exc:
            logger.warning(f"OpenFIGI response parse error for '{company_name}': {exc}")
            return None
