import io
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class NYScraper(BaseScraper):
    """New York DOL WARN Act scraper.

    NY publishes WARN notices as downloadable Excel files organized by year.
    """

    STATE = "NY"
    BASE_URL = "https://dol.ny.gov/warn-notices"

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        try:
            resp = self._get(self.BASE_URL)
            soup = BeautifulSoup(resp.text, "lxml")

            # Find Excel/CSV download links on the page
            download_links = []
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                    full_url = href if href.startswith("http") else f"https://dol.ny.gov{href}"
                    download_links.append(full_url)

            for url in download_links:
                try:
                    resp2 = self._get(url)
                    if url.lower().endswith(".csv"):
                        df = pd.read_csv(io.BytesIO(resp2.content))
                    else:
                        df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                    results.extend(self._parse_dataframe(df, url))
                except Exception as e:
                    self.logger.warning(f"Failed to parse {url}: {e}")

            # Fallback: try to read HTML tables directly
            if not results:
                results.extend(self._parse_html_tables(resp.text))

        except Exception as e:
            self.logger.error(f"NY scraper failed: {e}")

        self.logger.info(f"NY scraper found {len(results)} filings")
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
            if "company" in cl or "employer" in cl or "business" in cl:
                mapping["company"] = c
            elif "notice" in cl and "date" in cl:
                mapping["filing_date"] = c
            elif "warn" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "event" in cl and "date" in cl:
                mapping["layoff_date"] = c
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "employee" in cl or "worker" in cl or "number" in cl or "affected" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "region" in cl or "city" in cl or "location" in cl or "county" in cl:
                mapping["location"] = c
        return mapping

    def _parse_html_tables(self, html: str) -> List[Dict[str, Any]]:
        results = []
        try:
            tables = pd.read_html(html)
            for df in tables:
                if len(df) > 3:
                    results.extend(self._parse_dataframe(df, self.BASE_URL))
        except Exception:
            pass
        return results
