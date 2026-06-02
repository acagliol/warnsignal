import io
import re
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class ILScraper(BaseScraper):
    """Illinois WARN Act scraper.

    IL publishes WARN notices as monthly Excel reports on IllinoisWorkNet.
    The archive page contains download links for each monthly report.
    """

    STATE = "IL"
    BASE_URL = "https://www.illinoisworknet.com/LayoffRecovery/Pages/ArchivedWARNReports.aspx"
    ALT_URLS = [
        "https://dceo.illinois.gov/workforcedevelopment/warn.html",
        "https://ildceo.net/warn-notices",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Try primary URL: IllinoisWorkNet archive page
        try:
            results = self._scrape_worknet_archive()
        except Exception as e:
            self.logger.warning(f"IL WorkNet archive failed: {e}")

        # Fallback: try DCEO and ildceo.net
        if not results:
            for url in self.ALT_URLS:
                try:
                    resp = self._get(url)
                    parsed = self._scrape_generic_page(resp, url)
                    if parsed:
                        results.extend(parsed)
                        self.logger.info(f"IL alt source {url}: {len(parsed)} filings")
                        break
                except Exception as e:
                    self.logger.warning(f"IL alt source {url} failed: {e}")

        # Deduplicate
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"IL scraper found {len(unique)} unique filings")
        return unique

    def _scrape_worknet_archive(self) -> List[Dict[str, Any]]:
        """Scrape the IllinoisWorkNet WARN archive page for Excel downloads."""
        results = []
        resp = self._get(self.BASE_URL)
        soup = BeautifulSoup(resp.text, "lxml")

        # Find all Excel download links
        excel_links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()

            if any(ext in href.lower() for ext in [".xlsx", ".xls"]):
                full_url = href if href.startswith("http") else f"https://www.illinoisworknet.com{href}"
                excel_links.append(full_url)
            elif "warn" in text and "report" in text:
                # Some links may not have file extension in href
                full_url = href if href.startswith("http") else f"https://www.illinoisworknet.com{href}"
                excel_links.append(full_url)

        self.logger.info(f"IL: found {len(excel_links)} potential download links")

        for url in excel_links:
            try:
                resp2 = self._get(url)
                content_type = resp2.headers.get("Content-Type", "")

                if "html" in content_type and len(resp2.content) < 10000:
                    continue  # Skip error pages

                try:
                    df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                except Exception:
                    try:
                        df = pd.read_excel(io.BytesIO(resp2.content), engine="xlrd")
                    except Exception:
                        try:
                            df = pd.read_csv(io.BytesIO(resp2.content))
                        except Exception:
                            self.logger.warning(f"IL: could not parse {url}")
                            continue

                parsed = self._parse_dataframe(df, url)
                if parsed:
                    self.logger.info(f"IL: {url} -> {len(parsed)} filings")
                    results.extend(parsed)

                time.sleep(self.delay)
            except Exception as e:
                self.logger.warning(f"IL download {url} failed: {e}")

        return results

    def _scrape_generic_page(self, resp, base_url: str) -> List[Dict[str, Any]]:
        """Scrape a generic page for Excel/CSV links or HTML tables."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Look for downloadable files
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = href if href.startswith("http") else f"{base_url.rsplit('/', 1)[0]}/{href}"
                try:
                    resp2 = self._get(full_url)
                    if full_url.lower().endswith(".csv"):
                        df = pd.read_csv(io.BytesIO(resp2.content))
                    else:
                        df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                    results.extend(self._parse_dataframe(df, full_url))
                except Exception as e:
                    self.logger.warning(f"Failed to parse IL download {full_url}: {e}")

        # Try HTML tables
        if not results:
            try:
                tables = pd.read_html(resp.text)
                for df in tables:
                    if len(df) > 3:
                        results.extend(self._parse_dataframe(df, base_url))
            except ValueError:
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
            elif "received" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl:
                mapping["location"] = c
        return mapping
