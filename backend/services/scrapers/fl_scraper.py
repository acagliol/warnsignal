import io
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class FLScraper(BaseScraper):
    """Florida WARN Act scraper.

    FL publishes WARN notices via the REACT WARN system at
    reactwarn.floridajobs.org. Data is available by year with
    a download endpoint for Excel files.
    """

    STATE = "FL"
    BASE_URL = "https://reactwarn.floridajobs.org/WarnList/Records"
    DOWNLOAD_URL = "https://reactwarn.floridajobs.org/WarnList/Download"
    YEARS = [2026, 2025, 2024, 2023, 2022, 2021, 2020]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        for year in self.YEARS:
            # Try download endpoint first (Excel)
            try:
                url = f"{self.DOWNLOAD_URL}?year={year}"
                resp = self._get(url)
                content_type = resp.headers.get("Content-Type", "")

                if "spreadsheet" in content_type or "excel" in content_type or "octet-stream" in content_type:
                    try:
                        df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
                    except Exception:
                        df = pd.read_csv(io.BytesIO(resp.content))
                    parsed = self._parse_dataframe(df, url)
                    self.logger.info(f"FL {year} download: {len(parsed)} filings")
                    results.extend(parsed)
                    time.sleep(self.delay)
                    continue
                elif "csv" in content_type or "text" in content_type:
                    df = pd.read_csv(io.BytesIO(resp.content))
                    parsed = self._parse_dataframe(df, url)
                    self.logger.info(f"FL {year} download: {len(parsed)} filings")
                    results.extend(parsed)
                    time.sleep(self.delay)
                    continue
            except Exception as e:
                self.logger.warning(f"FL {year} download failed: {e}")

            # Fallback: scrape paginated HTML
            try:
                page = 1
                year_results = []
                while True:
                    url = f"{self.BASE_URL}?year={year}&page={page}"
                    resp = self._get(url)

                    if resp.status_code != 200:
                        break

                    soup = BeautifulSoup(resp.text, "lxml")

                    # Try HTML tables
                    try:
                        tables = pd.read_html(resp.text)
                        page_found = False
                        for df in tables:
                            if len(df) > 0:
                                parsed = self._parse_dataframe(df, url)
                                if parsed:
                                    year_results.extend(parsed)
                                    page_found = True

                        if not page_found:
                            break
                    except ValueError:
                        break

                    # Check for next page
                    next_link = soup.find("a", string=lambda t: t and "next" in t.lower() if t else False)
                    if not next_link and page > 1:
                        # Also check for numbered pagination
                        has_next = soup.find("a", href=lambda h: h and f"page={page + 1}" in h if h else False)
                        if not has_next:
                            break

                    page += 1
                    if page > 50:  # Safety limit
                        break
                    time.sleep(self.delay)

                self.logger.info(f"FL {year} HTML: {len(year_results)} filings")
                results.extend(year_results)
            except Exception as e:
                self.logger.warning(f"FL {year} HTML scrape failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"FL scraper found {len(unique)} unique filings")
        return unique

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
            if "company" in cl or "employer" in cl or "business" in cl:
                mapping["company"] = c
            elif "state" in cl and "notification" in cl:
                mapping["filing_date"] = c
            elif "notice" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "warn" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl or "industry" in cl:
                mapping.setdefault("location", c)
        return mapping
