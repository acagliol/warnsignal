import io
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class TXScraper(BaseScraper):
    """Texas TWC WARN Act scraper.

    TX publishes WARN data as Excel files with a known URL pattern.
    """

    STATE = "TX"
    BASE_URL = "https://twc.texas.gov/data-reports/warn-notice"
    EXCEL_BASE = "https://twc.texas.gov/sites/default/files/oei/docs"
    YEARS = [2026, 2025, 2024, 2023]

    def __init__(self, delay_seconds: float = 2.0):
        super().__init__(delay_seconds)
        self.session.headers.update({"User-Agent": "WARNSignal research@warnsignal.dev"})

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        for year in self.YEARS:
            url = f"{self.EXCEL_BASE}/warn-act-listings-{year}-twc.xlsx"
            try:
                resp = self._get(url)
                df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
                parsed = self._parse_dataframe(df, url)
                self.logger.info(f"TX {year}: {len(parsed)} filings")
                results.extend(parsed)
            except Exception as e:
                self.logger.warning(f"TX {year} Excel failed: {e}")

        self.logger.info(f"TX scraper found {len(results)} filings")
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
            if "company" in cl or "employer" in cl or "business" in cl or "organization" in cl or "job_site" in cl or "site_name" in cl or "facility" in cl:
                mapping["company"] = c
            elif "notice" in cl and "date" in cl:
                mapping["filing_date"] = c
            elif "warn" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "layoff" in cl and "date" in cl:
                mapping["layoff_date"] = c
            elif "closure" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl:
                mapping["location"] = c
        return mapping

    def _parse_with_bs4(self, html: str) -> List[Dict[str, Any]]:
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
                    "source_url": self.BASE_URL,
                })

        return results
