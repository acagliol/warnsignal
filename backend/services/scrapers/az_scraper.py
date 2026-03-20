import io
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class AZScraper(BaseScraper):
    """Arizona WARN Act scraper.

    AZ publishes WARN notices through AZJobConnection which has a web database
    search interface. We attempt to submit the form with broad parameters to
    retrieve all results, and also try alternate URL patterns.
    """

    STATE = "AZ"
    BASE_URL = "https://www.azjobconnection.gov/search/warn_lookups/new"
    SEARCH_URL = "https://www.azjobconnection.gov/search/warn_lookups"
    ALT_URLS = [
        "https://www.azcommerce.com/warn/",
        "https://www.azjobconnection.gov/warn",
        "https://www.azjobconnection.gov/ada/mn_warn_dsp.cfm",
        "https://des.az.gov/warn-notices",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Try the search interface first (submit broad query)
        try:
            search_results = self._scrape_search_interface()
            if search_results:
                self.logger.info(f"AZ search interface: {len(search_results)} filings")
                results.extend(search_results)
        except Exception as e:
            self.logger.warning(f"AZ search interface failed: {e}")

        # Try the primary URL as a regular page
        if not results:
            try:
                resp = self._get(self.BASE_URL)
                parsed = self._scrape_page(resp, self.BASE_URL)
                if parsed:
                    self.logger.info(f"AZ primary URL: {len(parsed)} filings")
                    results.extend(parsed)
            except Exception as e:
                self.logger.warning(f"AZ primary URL failed: {e}")

        # Try alternate URLs
        if not results:
            for url in self.ALT_URLS:
                try:
                    resp = self._get(url)
                    parsed = self._scrape_page(resp, url)
                    if parsed:
                        self.logger.info(f"AZ alt source {url}: {len(parsed)} filings")
                        results.extend(parsed)
                        break
                except Exception as e:
                    self.logger.warning(f"AZ alt source {url} failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"AZ scraper found {len(unique)} unique filings")
        return unique

    def _scrape_search_interface(self) -> List[Dict[str, Any]]:
        """Submit the AZJobConnection WARN search form with broad parameters."""
        results = []

        # First, load the search page to get any CSRF tokens or session cookies
        try:
            resp = self._get(self.BASE_URL)
            soup = BeautifulSoup(resp.text, "lxml")

            # Look for CSRF token or authenticity token
            token = None
            token_input = soup.find("input", {"name": "authenticity_token"})
            if token_input:
                token = token_input.get("value", "")

            # Also check for meta CSRF token
            if not token:
                meta = soup.find("meta", {"name": "csrf-token"})
                if meta:
                    token = meta.get("content", "")

            # Submit the search form with broad parameters (empty = all results)
            form_data = {}
            if token:
                form_data["authenticity_token"] = token

            # Try common form field names for AZJobConnection
            form_data.update({
                "utf8": "\u2713",
                "warn_lookup[company_name]": "",
                "warn_lookup[city]": "",
                "warn_lookup[county]": "",
                "commit": "Search",
            })

            self.logger.info("AZ: submitting search form")
            search_resp = self.session.post(
                self.SEARCH_URL,
                data=form_data,
                timeout=30,
                allow_redirects=True,
            )
            self._rate_limit()

            if search_resp.status_code == 200:
                # Parse the results page
                parsed = self._scrape_page(search_resp, self.SEARCH_URL)
                if parsed:
                    results.extend(parsed)

                # Check for pagination
                results.extend(self._scrape_search_pagination(search_resp))

        except Exception as e:
            self.logger.warning(f"AZ search form submission failed: {e}")

        return results

    def _scrape_search_pagination(self, resp) -> List[Dict[str, Any]]:
        """Follow pagination on search results."""
        results = []
        page = 2

        while page <= 50:  # Safety limit
            soup = BeautifulSoup(resp.text, "lxml")

            next_link = soup.find("a", href=lambda h: h and f"page={page}" in h if h else False)
            if not next_link:
                next_link = soup.find("a", {"rel": "next"})
            if not next_link:
                next_link = soup.find("a", string=lambda t: t and "next" in t.lower() if t else False)
            if not next_link:
                break

            href = next_link.get("href", "")
            if not href:
                break

            next_url = href if href.startswith("http") else self._resolve_url(self.SEARCH_URL, href)

            try:
                resp = self._get(next_url)

                try:
                    tables = pd.read_html(resp.text)
                    page_found = False
                    for df in tables:
                        if len(df) > 0:
                            parsed = self._parse_dataframe(df, next_url)
                            if parsed:
                                results.extend(parsed)
                                page_found = True
                    if not page_found:
                        break
                except ValueError:
                    # Try BS4 fallback
                    soup = BeautifulSoup(resp.text, "lxml")
                    parsed = self._parse_with_bs4(soup, next_url)
                    if not parsed:
                        break
                    results.extend(parsed)

                page += 1
                time.sleep(self.delay)
            except Exception as e:
                self.logger.warning(f"AZ pagination page {page} failed: {e}")
                break

        return results

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a single AZ WARN page for download links and HTML tables."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Find Excel/CSV download links
        download_links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()

            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = href if href.startswith("http") else self._resolve_url(page_url, href)
                download_links.append(full_url)
            elif "export" in text or "download" in text:
                full_url = href if href.startswith("http") else self._resolve_url(page_url, href)
                download_links.append(full_url)

        for url in download_links:
            try:
                resp2 = self._get(url)
                content_type = resp2.headers.get("Content-Type", "")

                if "html" in content_type and len(resp2.content) < 10000:
                    continue

                if url.lower().endswith(".csv") or "csv" in content_type:
                    df = pd.read_csv(io.BytesIO(resp2.content))
                else:
                    try:
                        df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                    except Exception:
                        try:
                            df = pd.read_excel(io.BytesIO(resp2.content), engine="xlrd")
                        except Exception:
                            df = pd.read_csv(io.BytesIO(resp2.content))

                parsed = self._parse_dataframe(df, url)
                if parsed:
                    self.logger.info(f"AZ: {url} -> {len(parsed)} filings")
                    results.extend(parsed)

                time.sleep(self.delay)
            except Exception as e:
                self.logger.warning(f"AZ download {url} failed: {e}")

        # Also try HTML tables on the page
        try:
            tables = pd.read_html(resp.text)
            for df in tables:
                if len(df) > 3:
                    results.extend(self._parse_dataframe(df, page_url))
        except (ValueError, Exception):
            pass

        # Fallback: parse tables with BeautifulSoup directly
        if not results:
            results.extend(self._parse_with_bs4(soup, page_url))

        return results

    def _parse_with_bs4(self, soup: BeautifulSoup, source_url: str) -> List[Dict[str, Any]]:
        """Fallback parser using BeautifulSoup for non-standard table layouts."""
        results = []
        tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")
            if len(rows) < 3:
                continue

            headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
            col_map = self._detect_columns(headers)
            if not col_map.get("company"):
                continue

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cells) < len(headers):
                    continue

                row_dict = dict(zip(headers, cells))
                company = row_dict.get(col_map["company"], "").strip()
                if not company:
                    continue

                results.append({
                    "company_name": company,
                    "filing_date": self.parse_date(row_dict.get(col_map.get("filing_date", ""))),
                    "layoff_date": self.parse_date(row_dict.get(col_map.get("layoff_date", ""))),
                    "employees_affected": self.parse_int(row_dict.get(col_map.get("employees", ""))),
                    "location": row_dict.get(col_map.get("location", "")) or None,
                    "source_url": source_url,
                })

        return results

    def _parse_dataframe(self, df: pd.DataFrame, source_url: str) -> List[Dict[str, Any]]:
        records = []
        df.columns = [str(c).strip().lower() for c in df.columns]
        col_map = self._detect_columns(df.columns.tolist())

        if not col_map.get("company"):
            return records

        for _, row in df.iterrows():
            company = str(row.get(col_map["company"], "")).strip()
            if not company or company.lower() == "nan":
                continue

            records.append({
                "company_name": company,
                "filing_date": self.parse_date(row.get(col_map.get("filing_date", ""), None)),
                "layoff_date": self.parse_date(row.get(col_map.get("layoff_date", ""), None)),
                "employees_affected": self.parse_int(row.get(col_map.get("employees", ""), None)),
                "location": str(row.get(col_map.get("location", ""), "")).strip() or None,
                "source_url": source_url,
            })

        return records

    def _detect_columns(self, cols: list) -> dict:
        mapping = {}
        for c in cols:
            cl = c.lower()
            if "company" in cl or "employer" in cl or "business" in cl or "organization" in cl:
                mapping["company"] = c
            elif "notice" in cl and "date" in cl:
                mapping["filing_date"] = c
            elif "warn" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "received" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "date" in cl and "filed" in cl:
                mapping.setdefault("filing_date", c)
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "closure" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl or "area" in cl:
                mapping.setdefault("location", c)
        return mapping

    @staticmethod
    def _resolve_url(base_url: str, href: str) -> str:
        """Resolve a relative URL against a base URL."""
        from urllib.parse import urljoin
        return urljoin(base_url, href)
