import io
from typing import List, Dict, Any
import pandas as pd
from services.scrapers.base_scraper import BaseScraper


class CAScraper(BaseScraper):
    """California EDD WARN Act scraper.

    CA publishes WARN data as downloadable Excel/CSV files.
    We try the data page and look for Excel download links.
    """

    STATE = "CA"
    BASE_URL = "https://edd.ca.gov/en/Jobs_and_Training/Layoff_Services_WARN"

    # Direct data URLs (CA EDD provides XLSX downloads for recent years)
    DATA_URLS = [
        "https://edd.ca.gov/siteassets/files/jobs_and_training/warn/warn_report.xlsx",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        for url in self.DATA_URLS:
            try:
                resp = self._get(url)
                xl = pd.ExcelFile(io.BytesIO(resp.content), engine="openpyxl")
                sheet = next((s for s in xl.sheet_names if "detailed" in s.lower()), xl.sheet_names[-1])
                df = xl.parse(sheet, header=1)
                results.extend(self._parse_dataframe(df, url))
            except Exception as e:
                self.logger.warning(f"Failed to fetch {url}: {e}")
                # Fallback: try to scrape HTML page for download links
                try:
                    results.extend(self._scrape_html_fallback())
                except Exception as e2:
                    self.logger.error(f"HTML fallback also failed: {e2}")

        self.logger.info(f"CA scraper found {len(results)} filings")
        return results

    def _parse_dataframe(self, df: pd.DataFrame, source_url: str) -> List[Dict[str, Any]]:
        records = []
        # Normalize column names
        df.columns = [str(c).strip().lower() for c in df.columns]

        # Map CA EDD column names to standard fields
        col_map = self._detect_columns(df.columns.tolist())
        if not col_map.get("company"):
            self.logger.warning("Could not detect company column in CA data")
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
        """Auto-detect column name mapping from header row."""
        mapping = {}
        for c in cols:
            cl = c.lower().replace("\n", " ")
            if "company" in cl or "employer" in cl or "business" in cl:
                mapping["company"] = c
            elif "notice" in cl and "date" in cl:
                mapping["filing_date"] = c
            elif "received" in cl and "date" in cl:
                mapping["filing_date"] = c
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "employee" in cl or "worker" in cl or "no." in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl or "parish" in cl or "address" in cl:
                if "location" not in mapping:
                    mapping["location"] = c
        return mapping

    def _scrape_html_fallback(self) -> List[Dict[str, Any]]:
        """Fallback: parse HTML tables from the main WARN page."""
        from bs4 import BeautifulSoup

        resp = self._get(self.BASE_URL)
        soup = BeautifulSoup(resp.text, "lxml")

        # Try to find Excel/CSV download links
        results = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.endswith((".xlsx", ".xls", ".csv")):
                full_url = href if href.startswith("http") else f"https://edd.ca.gov{href}"
                if full_url not in self.DATA_URLS:
                    try:
                        resp2 = self._get(full_url)
                        if full_url.endswith(".csv"):
                            df = pd.read_csv(io.BytesIO(resp2.content))
                        else:
                            df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                        results.extend(self._parse_dataframe(df, full_url))
                    except Exception as e:
                        self.logger.warning(f"Failed to parse {full_url}: {e}")

        # Also try pandas.read_html on the page itself
        try:
            tables = pd.read_html(resp.text)
            for df in tables:
                if len(df) > 5:
                    results.extend(self._parse_dataframe(df, self.BASE_URL))
        except Exception:
            pass

        return results
