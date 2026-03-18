import io
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class FLScraper(BaseScraper):
    """Florida DEO WARN Act scraper.

    FL publishes WARN notices via HTML tables or downloadable files.
    Falls back to PDF parsing with pdfplumber if needed.
    """

    STATE = "FL"
    BASE_URL = (
        "https://floridajobs.org/office-directory/division-of-workforce-services/"
        "workforce-programs/reemployment-and-emergency-assistance-coordination-team-react/"
        "warn-notices"
    )

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        try:
            resp = self._get(self.BASE_URL)
            soup = BeautifulSoup(resp.text, "lxml")

            # Look for downloadable files first
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                    full_url = href if href.startswith("http") else f"https://floridajobs.org{href}"
                    try:
                        resp2 = self._get(full_url)
                        if full_url.lower().endswith(".csv"):
                            df = pd.read_csv(io.BytesIO(resp2.content))
                        else:
                            df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                        results.extend(self._parse_dataframe(df, full_url))
                    except Exception as e:
                        self.logger.warning(f"Failed to parse FL download {full_url}: {e}")

                elif href.lower().endswith(".pdf"):
                    full_url = href if href.startswith("http") else f"https://floridajobs.org{href}"
                    try:
                        results.extend(self._parse_pdf(full_url))
                    except Exception as e:
                        self.logger.warning(f"Failed to parse FL PDF {full_url}: {e}")

            # Try HTML tables on page
            if not results:
                try:
                    tables = pd.read_html(resp.text)
                    for df in tables:
                        if len(df) > 3:
                            results.extend(self._parse_dataframe(df, self.BASE_URL))
                except ValueError:
                    pass

        except Exception as e:
            self.logger.error(f"FL scraper failed: {e}")

        self.logger.info(f"FL scraper found {len(results)} filings")
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
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl:
                mapping["location"] = c
        return mapping

    def _parse_pdf(self, url: str) -> List[Dict[str, Any]]:
        """Parse WARN data from a PDF file using pdfplumber."""
        import pdfplumber

        resp = self._get(url)
        results = []

        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if len(table) < 2:
                        continue

                    headers = [str(h).strip().lower() if h else "" for h in table[0]]
                    col_map = self._detect_columns(headers)
                    if not col_map.get("company"):
                        continue

                    for row in table[1:]:
                        row_dict = dict(zip(headers, [str(v).strip() if v else "" for v in row]))
                        company = row_dict.get(col_map["company"], "").strip()
                        if not company:
                            continue

                        results.append({
                            "company_name": company,
                            "filing_date": self.parse_date(row_dict.get(col_map.get("filing_date", ""))),
                            "layoff_date": self.parse_date(row_dict.get(col_map.get("layoff_date", ""))),
                            "employees_affected": self.parse_int(row_dict.get(col_map.get("employees", ""))),
                            "location": row_dict.get(col_map.get("location", "")) or None,
                            "source_url": url,
                        })

        return results
