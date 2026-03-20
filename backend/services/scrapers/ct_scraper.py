import io
import re
import time
from typing import List, Dict, Any
from urllib.parse import urljoin
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class CTScraper(BaseScraper):
    """Connecticut DOL WARN Act scraper.

    CT publishes WARN data as HTML pages with links to individual
    year reports. Each year page contains HTML tables with WARN data.
    """

    STATE = "CT"
    BASE_URL = "https://www.ctdol.state.ct.us/progsupt/bussrvce/warnreports/warnreports.htm"
    ALT_BASE = "https://www.ctdol.state.ct.us/progsupt/bussrvce/warnreports"
    YEARS = [2026, 2025, 2024, 2023, 2022, 2021, 2020]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Try the main index page first to discover year links
        try:
            resp = self._get(self.BASE_URL)
            index_results = self._scrape_index_page(resp)
            results.extend(index_results)
        except Exception as e:
            self.logger.warning(f"CT index page failed: {e}")

        # Also try direct year URL patterns
        year_url_patterns = [
            "{base}/warn{year}.htm",
            "{base}/warn{year}.html",
            "{base}/{year}warnnotices.htm",
            "{base}/{year}-warn-notices.htm",
            "{base}/WARN{year}.htm",
        ]

        for year in self.YEARS:
            for pattern in year_url_patterns:
                url = pattern.format(base=self.ALT_BASE, year=year)
                try:
                    resp = self._get(url)
                    parsed = self._scrape_page(resp, url)
                    if parsed:
                        self.logger.info(f"CT {year} ({url}): {len(parsed)} filings")
                        results.extend(parsed)
                        break  # Found data for this year, skip other patterns
                except Exception:
                    continue

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"CT scraper found {len(unique)} unique filings")
        return unique

    def _scrape_index_page(self, resp) -> List[Dict[str, Any]]:
        """Scrape the CT WARN index page for links to year reports."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Find all links that look like year report pages
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()

            # Match links containing year numbers or "warn" in text
            is_year_link = any(str(y) in text or str(y) in href for y in self.YEARS)
            is_warn_link = "warn" in text or "warn" in href.lower()

            if is_year_link or is_warn_link:
                full_url = href if href.startswith("http") else urljoin(self.BASE_URL, href)

                # Skip the index page itself
                if full_url == self.BASE_URL:
                    continue

                try:
                    resp2 = self._get(full_url)

                    # Check if it's a downloadable file
                    if any(ext in full_url.lower() for ext in [".xlsx", ".xls", ".csv"]):
                        try:
                            if full_url.lower().endswith(".csv"):
                                df = pd.read_csv(io.BytesIO(resp2.content))
                            else:
                                try:
                                    df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                                except Exception:
                                    df = pd.read_excel(io.BytesIO(resp2.content), engine="xlrd")
                            results.extend(self._parse_dataframe(df, full_url))
                        except Exception as e:
                            self.logger.warning(f"CT download {full_url} failed: {e}")
                    else:
                        # HTML page with tables
                        parsed = self._scrape_page(resp2, full_url)
                        if parsed:
                            results.extend(parsed)
                except Exception as e:
                    self.logger.warning(f"CT link {full_url} failed: {e}")

        # Also parse tables on the index page itself
        parsed = self._scrape_page(resp, self.BASE_URL)
        if parsed:
            results.extend(parsed)

        return results

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a single CT WARN page for HTML tables."""
        results = []

        # Try pandas.read_html first
        try:
            tables = pd.read_html(resp.text)
            for df in tables:
                if len(df) > 3:
                    parsed = self._parse_dataframe(df, page_url)
                    if parsed:
                        results.extend(parsed)
        except ValueError:
            pass

        # Fallback: BeautifulSoup table parsing
        if not results:
            results.extend(self._parse_with_bs4(resp.text, page_url))

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
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "closing" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "separation" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl or "town" in cl:
                if "location" not in mapping:
                    mapping["location"] = c
        return mapping

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
