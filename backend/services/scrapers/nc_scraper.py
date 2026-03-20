import io
import time
from typing import List, Dict, Any
from urllib.parse import urljoin
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class NCScraper(BaseScraper):
    """North Carolina Commerce WARN Act scraper.

    NC publishes WARN data via the Department of Commerce website.
    Data may be available as downloadable reports (Excel/CSV) or
    as HTML tables on the workforce WARN reports page.
    """

    STATE = "NC"
    BASE_URL = "https://www.commerce.nc.gov/data-tools-reports/labor-market-data/workforce-warn-reports"
    ALT_URLS = [
        "https://www.nccommerce.com/data-tools-reports/labor-market-data/workforce-warn-reports",
        "https://www.commerce.nc.gov/jobs-training/warn-notices",
        "https://www.nccommerce.com/jobs-training/warn-notices",
        "https://d4.nccommerce.com/WarnReport.aspx",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Try primary URL
        try:
            resp = self._get(self.BASE_URL)
            parsed = self._scrape_page(resp, self.BASE_URL)
            if parsed:
                self.logger.info(f"NC main page: {len(parsed)} filings")
                results.extend(parsed)
        except Exception as e:
            self.logger.warning(f"NC main page failed: {e}")

        # Try alternative URLs if primary yielded no results
        if not results:
            for url in self.ALT_URLS:
                try:
                    resp = self._get(url)
                    parsed = self._scrape_page(resp, url)
                    if parsed:
                        self.logger.info(f"NC alt source {url}: {len(parsed)} filings")
                        results.extend(parsed)
                        break
                except Exception as e:
                    self.logger.warning(f"NC alt source {url} failed: {e}")

        # Try the D4 report tool (may have a form-based interface)
        if not results:
            try:
                results.extend(self._scrape_d4_report())
            except Exception as e:
                self.logger.warning(f"NC D4 report tool failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"NC scraper found {len(unique)} unique filings")
        return unique

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a NC WARN page for download links and HTML tables."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Look for downloadable files (Excel, CSV, PDF)
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = href if href.startswith("http") else urljoin(page_url, href)
                try:
                    resp2 = self._get(full_url)
                    if full_url.lower().endswith(".csv"):
                        df = pd.read_csv(io.BytesIO(resp2.content))
                    else:
                        try:
                            df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                        except Exception:
                            try:
                                df = pd.read_excel(io.BytesIO(resp2.content), engine="xlrd")
                            except Exception:
                                df = pd.read_csv(io.BytesIO(resp2.content))
                    parsed = self._parse_dataframe(df, full_url)
                    if parsed:
                        results.extend(parsed)
                except Exception as e:
                    self.logger.warning(f"NC download {full_url} failed: {e}")

        # Also look for links to sub-pages containing WARN data
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()
            if "warn" in text and any(str(y) in text or str(y) in href for y in range(2020, 2027)):
                full_url = href if href.startswith("http") else urljoin(page_url, href)
                if full_url == page_url:
                    continue
                try:
                    resp2 = self._get(full_url)
                    sub_parsed = self._scrape_sub_page(resp2, full_url)
                    if sub_parsed:
                        results.extend(sub_parsed)
                except Exception as e:
                    self.logger.warning(f"NC sub-page {full_url} failed: {e}")

        # Parse HTML tables on the main page
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
            results.extend(self._parse_with_bs4(soup, page_url))

        return results

    def _scrape_sub_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a sub-page for WARN data tables and downloads."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Check for downloads
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = href if href.startswith("http") else urljoin(page_url, href)
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
                    self.logger.warning(f"NC sub-page download {full_url} failed: {e}")

        # Parse HTML tables
        try:
            tables = pd.read_html(resp.text)
            for df in tables:
                if len(df) > 3:
                    results.extend(self._parse_dataframe(df, page_url))
        except ValueError:
            pass

        return results

    def _scrape_d4_report(self) -> List[Dict[str, Any]]:
        """Try the D4 WARN report tool (ASP.NET form-based)."""
        results = []
        url = "https://d4.nccommerce.com/WarnReport.aspx"

        try:
            resp = self._get(url)

            # Try to parse any tables already on the page
            try:
                tables = pd.read_html(resp.text)
                for df in tables:
                    if len(df) > 3:
                        results.extend(self._parse_dataframe(df, url))
            except ValueError:
                pass
        except Exception as e:
            self.logger.warning(f"NC D4 report failed: {e}")

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
            elif "initial" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl or "lwda" in cl:
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
