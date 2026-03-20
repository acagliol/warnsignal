import io
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class NYScraper(BaseScraper):
    """New York DOL WARN Act scraper.

    NY publishes WARN notices as downloadable Excel files organized by year.
    The main page has current year data, with links to prior year pages.
    """

    STATE = "NY"
    YEAR_URLS = [
        "https://dol.ny.gov/warn-notices",
        "https://dol.ny.gov/2025-warn-notices",
        "https://dol.ny.gov/2024-warn-notices",
        "https://dol.ny.gov/2023-warn-notices",
        "https://dol.ny.gov/2022-warn-notices",
        "https://dol.ny.gov/2021-warn-notices",
        "https://dol.ny.gov/2020-warn-notices",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        for url in self.YEAR_URLS:
            try:
                resp = self._get(url)
                if resp.status_code != 200:
                    self.logger.warning(f"NY {url}: status {resp.status_code}")
                    continue

                page_results = self._scrape_page(resp, url)
                self.logger.info(f"NY {url}: {len(page_results)} filings")
                results.extend(page_results)
                time.sleep(self.delay)
            except Exception as e:
                self.logger.warning(f"NY {url} failed: {e}")

        # Also check for a WARN Dashboard
        try:
            dashboard_url = "https://dol.ny.gov/warn-dashboard"
            resp = self._get(dashboard_url)
            if resp.status_code == 200:
                dash_results = self._scrape_page(resp, dashboard_url)
                if dash_results:
                    self.logger.info(f"NY dashboard: {len(dash_results)} filings")
                    results.extend(dash_results)
        except Exception as e:
            self.logger.debug(f"NY dashboard not available: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"NY scraper found {len(unique)} unique filings")
        return unique

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a single NY WARN page for Excel links and HTML tables."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Find Excel/CSV download links
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

        # Also try HTML tables on the page
        try:
            tables = pd.read_html(resp.text)
            for df in tables:
                if len(df) > 3:
                    results.extend(self._parse_dataframe(df, page_url))
        except (ValueError, Exception):
            pass

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
