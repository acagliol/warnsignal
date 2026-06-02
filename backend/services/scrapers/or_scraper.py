import io
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class ORScraper(BaseScraper):
    """Oregon WARN Act scraper.

    OR publishes layoff and WARN notice data through QualityInfo.org and
    the HECC (Higher Education Coordinating Commission) CCWD site.
    Data may be available via a database search interface or downloadable files.
    """

    STATE = "OR"
    BASE_URL = "https://www.qualityinfo.org/layoff-information/"
    ALT_URLS = [
        "https://ccwd.hecc.oregon.gov/Layoff/WARN",
        "https://ccwd.hecc.oregon.gov/Layoff",
        "https://www.qualityinfo.org/layoff-information",
        "https://www.oregon.gov/employ/pages/warn.aspx",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Try primary URL first
        try:
            resp = self._get(self.BASE_URL)
            parsed = self._scrape_page(resp, self.BASE_URL)
            if parsed:
                self.logger.info(f"OR primary URL: {len(parsed)} filings")
                results.extend(parsed)
        except Exception as e:
            self.logger.warning(f"OR primary URL failed: {e}")

        # Try HECC/CCWD site which may have a database interface
        try:
            ccwd_results = self._scrape_ccwd()
            if ccwd_results:
                self.logger.info(f"OR CCWD: {len(ccwd_results)} filings")
                results.extend(ccwd_results)
        except Exception as e:
            self.logger.warning(f"OR CCWD scrape failed: {e}")

        # Try remaining alternate URLs if still no results
        if not results:
            for url in self.ALT_URLS[2:]:
                try:
                    resp = self._get(url)
                    parsed = self._scrape_page(resp, url)
                    if parsed:
                        self.logger.info(f"OR alt source {url}: {len(parsed)} filings")
                        results.extend(parsed)
                        break
                except Exception as e:
                    self.logger.warning(f"OR alt source {url} failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"OR scraper found {len(unique)} unique filings")
        return unique

    def _scrape_ccwd(self) -> List[Dict[str, Any]]:
        """Scrape the HECC CCWD layoff/WARN database."""
        results = []
        ccwd_url = "https://ccwd.hecc.oregon.gov/Layoff/WARN"

        try:
            resp = self._get(ccwd_url)
            soup = BeautifulSoup(resp.text, "lxml")

            # Try to find a data endpoint or API URL in the page
            # CCWD may use AJAX to load data
            scripts = soup.find_all("script")
            for script in scripts:
                text = script.string or ""
                if "api" in text.lower() or "dataurl" in text.lower() or "ajax" in text.lower():
                    self.logger.info(f"OR CCWD: found potential API reference in script")

            # Try HTML tables first
            try:
                tables = pd.read_html(resp.text)
                for df in tables:
                    if len(df) > 3:
                        results.extend(self._parse_dataframe(df, ccwd_url))
            except (ValueError, Exception):
                pass

            # Try BeautifulSoup fallback
            if not results:
                results.extend(self._parse_with_bs4(soup, ccwd_url))

            # Look for download links on the page
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                    full_url = href if href.startswith("http") else self._resolve_url(ccwd_url, href)
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
                        self.logger.warning(f"OR CCWD download {full_url} failed: {e}")

        except Exception as e:
            self.logger.warning(f"OR CCWD page failed: {e}")

        return results

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a single OR WARN page for download links and HTML tables."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Find Excel/CSV download links
        download_links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()

            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = href if href.startswith("http") else self._resolve_url(page_url, href)
                download_links.append(full_url)
            elif "warn" in text and ("download" in text or "export" in text or "data" in text):
                full_url = href if href.startswith("http") else self._resolve_url(page_url, href)
                download_links.append(full_url)

        for url in download_links:
            try:
                resp2 = self._get(url)
                content_type = resp2.headers.get("Content-Type", "")

                if "html" in content_type and len(resp2.content) < 10000:
                    continue

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
                    self.logger.info(f"OR: {url} -> {len(parsed)} filings")
                    results.extend(parsed)

                time.sleep(self.delay)
            except Exception as e:
                self.logger.warning(f"OR download {url} failed: {e}")

        # Also try HTML tables on the page
        try:
            tables = pd.read_html(resp.text)
            for df in tables:
                if len(df) > 3:
                    results.extend(self._parse_dataframe(df, page_url))
        except (ValueError, Exception):
            pass

        # Fallback: parse tables with BeautifulSoup directly
        if not results:
            results.extend(self._parse_with_bs4(soup, page_url))

        return results

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
            if "company" in cl or "employer" in cl or "business" in cl or "organization" in cl or "firm" in cl:
                mapping["company"] = c
            elif "notice" in cl and "date" in cl:
                mapping["filing_date"] = c
            elif "warn" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "received" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "initial" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "closure" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl or "area" in cl:
                mapping.setdefault("location", c)
        return mapping

    @staticmethod
    def _resolve_url(base_url: str, href: str) -> str:
        """Resolve a relative URL against a base URL."""
        from urllib.parse import urljoin
        return urljoin(base_url, href)
