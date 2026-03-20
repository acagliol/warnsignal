import io
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class MDScraper(BaseScraper):
    """Maryland DLLR WARN Act scraper.

    MD publishes WARN data as HTML tables on the DLLR website.
    The main page may contain current data, with separate pages
    for prior years.
    """

    STATE = "MD"
    BASE_URL = "https://www.dllr.state.md.us/employment/warn.shtml"
    ALT_URLS = [
        "https://www.dllr.state.md.us/employment/warnarchive.shtml",
        "https://www.dllr.state.md.us/employment/warn2025.shtml",
        "https://www.dllr.state.md.us/employment/warn2024.shtml",
        "https://www.dllr.state.md.us/employment/warn2023.shtml",
        "https://www.dllr.state.md.us/employment/warn2022.shtml",
        "https://www.dllr.state.md.us/employment/warn2021.shtml",
        "https://www.dllr.state.md.us/employment/warn2020.shtml",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Scrape main WARN page
        try:
            resp = self._get(self.BASE_URL)
            parsed = self._scrape_page(resp, self.BASE_URL)
            self.logger.info(f"MD main page: {len(parsed)} filings")
            results.extend(parsed)
        except Exception as e:
            self.logger.warning(f"MD main page failed: {e}")

        # Try year-specific and archive pages
        for url in self.ALT_URLS:
            try:
                resp = self._get(url)
                parsed = self._scrape_page(resp, url)
                if parsed:
                    self.logger.info(f"MD {url}: {len(parsed)} filings")
                    results.extend(parsed)
            except Exception as e:
                self.logger.warning(f"MD {url} failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"MD scraper found {len(unique)} unique filings")
        return unique

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a single MD WARN page for HTML tables and download links."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Check for Excel/CSV download links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = href if href.startswith("http") else f"https://www.dllr.state.md.us{href}"
                try:
                    resp2 = self._get(full_url)
                    if full_url.lower().endswith(".csv"):
                        df = pd.read_csv(io.BytesIO(resp2.content))
                    else:
                        try:
                            df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                        except Exception:
                            df = pd.read_excel(io.BytesIO(resp2.content), engine="xlrd")
                    results.extend(self._parse_dataframe(df, full_url))
                except Exception as e:
                    self.logger.warning(f"MD download {full_url} failed: {e}")

        # Parse HTML tables with pandas
        try:
            tables = pd.read_html(resp.text)
            for df in tables:
                if len(df) > 3:
                    parsed = self._parse_dataframe(df, page_url)
                    if parsed:
                        results.extend(parsed)
        except ValueError:
            pass

        # Fallback: parse tables with BeautifulSoup
        if not results:
            results.extend(self._parse_with_bs4(soup, page_url))

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
            elif "received" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "closing" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl or "area" in cl:
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
