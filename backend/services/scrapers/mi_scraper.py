import io
import re
from typing import List, Dict, Any
from urllib.parse import urljoin
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class MIScraper(BaseScraper):
    """Michigan WARN Act scraper.

    MI publishes WARN notices through the Department of Labor and Economic
    Opportunity (LEO) and the Michigan Labor Market Information (MILMI) site.
    Data may be available as HTML tables or downloadable Excel/CSV files.
    """

    STATE = "MI"
    BASE_URL = "https://milmi.org/warn-notices"
    ALT_URLS = [
        "https://milmi.org/datasearch/warn-notices",
        "https://www.michigan.gov/leo/bureaus-agencies/wd/warn-notices",
        "https://www.michigan.gov/leo/bureaus-agencies/workforce-development/warn-notices",
        "https://www.michigan.gov/leo/warn-notices",
        "https://milmi.org/warn",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Try primary URL first
        try:
            resp = self._get(self.BASE_URL)
            parsed = self._scrape_page(resp, self.BASE_URL)
            if parsed:
                self.logger.info(f"MI primary URL: {len(parsed)} filings")
                results.extend(parsed)
        except Exception as e:
            self.logger.warning(f"MI primary URL failed: {e}")

        # Try alternate URLs if primary yielded nothing
        if not results:
            for url in self.ALT_URLS:
                try:
                    resp = self._get(url)
                    parsed = self._scrape_page(resp, url)
                    if parsed:
                        self.logger.info(f"MI alt source {url}: {len(parsed)} filings")
                        results.extend(parsed)
                        break
                except Exception as e:
                    self.logger.warning(f"MI alt source {url} failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"MI scraper found {len(unique)} unique filings")
        return unique

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a single MI WARN page for download links and HTML tables."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Strategy 1: Find Excel/CSV download links
        download_links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = href if href.startswith("http") else urljoin(page_url, href)
                download_links.append(full_url)

        # Also look for links whose text suggests a downloadable file
        for link in soup.find_all("a", href=True):
            text = link.get_text(strip=True).lower()
            href = link["href"]
            if any(kw in text for kw in ["download", "excel", "spreadsheet", "warn data", "warn list"]):
                full_url = href if href.startswith("http") else urljoin(page_url, href)
                if full_url not in download_links:
                    download_links.append(full_url)

        for url in download_links:
            try:
                resp2 = self._get(url)
                content_type = resp2.headers.get("Content-Type", "")

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
                    results.extend(parsed)
            except Exception as e:
                self.logger.warning(f"MI failed to parse download {url}: {e}")

        # Strategy 2: Parse HTML tables on the page using pandas
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

        # Strategy 3: BeautifulSoup fallback for non-standard tables
        if not results:
            results.extend(self._parse_with_bs4(soup, page_url))

        return results

    def _parse_dataframe(self, df: pd.DataFrame, source_url: str) -> List[Dict[str, Any]]:
        """Parse a DataFrame into standardized WARN records."""
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
        """Map column names to standardized field names."""
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
            elif "separation" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "dislocation" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl or "region" in cl or "area" in cl:
                if "location" not in mapping:
                    mapping["location"] = c
        return mapping

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
