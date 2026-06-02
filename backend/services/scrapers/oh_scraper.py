import io
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class OHScraper(BaseScraper):
    """Ohio WARN Act scraper.

    OH publishes WARN notices through the Department of Job and Family Services.
    Data is typically available as HTML tables on the WARN notices page.
    Multiple URL patterns are tried since government sites change frequently.
    """

    STATE = "OH"
    BASE_URL = "https://jfs.ohio.gov/job-services-and-unemployment/unemployment/workers/warn-notices"

    ALT_URLS = [
        "https://jfs.ohio.gov/warn/",
        "https://jfs.ohio.gov/ouio/warn-notices",
        "https://jfs.ohio.gov/owd/warn/",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Strategy 1: Try the primary URL
        try:
            resp = self._get(self.BASE_URL)
            parsed = self._scrape_page(resp, self.BASE_URL)
            if parsed:
                self.logger.info(f"OH primary URL: {len(parsed)} filings")
                results.extend(parsed)
        except Exception as e:
            self.logger.warning(f"OH primary URL failed: {e}")

        # Strategy 2: Try alternate URLs if primary yielded nothing
        if not results:
            for url in self.ALT_URLS:
                try:
                    resp = self._get(url)
                    parsed = self._scrape_page(resp, url)
                    if parsed:
                        self.logger.info(f"OH alt URL {url}: {len(parsed)} filings")
                        results.extend(parsed)
                        break
                except Exception as e:
                    self.logger.warning(f"OH alt URL {url} failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"OH scraper found {len(unique)} unique filings")
        return unique

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a single OH WARN page for download links and HTML tables."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Look for Excel/CSV download links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = href if href.startswith("http") else f"https://jfs.ohio.gov{href}"
                try:
                    resp2 = self._get(full_url)
                    if full_url.lower().endswith(".csv"):
                        df = pd.read_csv(io.BytesIO(resp2.content))
                    else:
                        try:
                            df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                        except Exception:
                            df = pd.read_excel(io.BytesIO(resp2.content), engine="xlrd")
                    parsed = self._parse_dataframe(df, full_url)
                    if parsed:
                        results.extend(parsed)
                        self.logger.info(f"OH download {full_url}: {len(parsed)} filings")
                except Exception as e:
                    self.logger.warning(f"OH download {full_url} failed: {e}")
                time.sleep(self.delay)

        # Try parsing HTML tables with pandas
        if not results:
            try:
                tables = pd.read_html(resp.text)
                for df in tables:
                    if len(df) > 3:
                        parsed = self._parse_dataframe(df, page_url)
                        if parsed:
                            results.extend(parsed)
            except (ValueError, Exception):
                pass

        # Fallback: parse with BeautifulSoup directly
        if not results:
            results = self._parse_with_bs4(resp.text, page_url)

        # Try to find sub-pages (OH sometimes links to year-specific pages)
        if not results:
            year_links = []
            for link in soup.find_all("a", href=True):
                href = link["href"]
                text = link.get_text(strip=True).lower()
                if ("warn" in text or "notice" in text) and any(
                    str(y) in text or str(y) in href for y in range(2020, 2027)
                ):
                    full_url = href if href.startswith("http") else f"https://jfs.ohio.gov{href}"
                    if full_url != page_url:
                        year_links.append(full_url)

            for url in year_links[:10]:  # Safety limit
                try:
                    resp2 = self._get(url)
                    parsed = self._scrape_sub_page(resp2, url)
                    if parsed:
                        results.extend(parsed)
                        self.logger.info(f"OH sub-page {url}: {len(parsed)} filings")
                except Exception as e:
                    self.logger.warning(f"OH sub-page {url} failed: {e}")
                time.sleep(self.delay)

        return results

    def _scrape_sub_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a sub-page (year-specific) for tables and downloads."""
        results = []

        # Try HTML tables
        try:
            tables = pd.read_html(resp.text)
            for df in tables:
                if len(df) > 3:
                    parsed = self._parse_dataframe(df, page_url)
                    if parsed:
                        results.extend(parsed)
        except (ValueError, Exception):
            pass

        # Fallback to BS4
        if not results:
            results = self._parse_with_bs4(resp.text, page_url)

        return results

    def _parse_with_bs4(self, html: str, source_url: str) -> List[Dict[str, Any]]:
        """Fallback parser using BeautifulSoup for non-standard table layouts."""
        results = []
        soup = BeautifulSoup(html, "lxml")
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
            elif "filed" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "closure" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl:
                mapping["location"] = c
        return mapping
