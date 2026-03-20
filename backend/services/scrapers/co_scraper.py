import io
import re
import time
from typing import List, Dict, Any
from urllib.parse import urljoin
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class COScraper(BaseScraper):
    """Colorado CDLE WARN Act scraper.

    CO publishes WARN data via the CDLE website, often using an
    embedded Google Sheet. We attempt to extract the Google Sheets
    URL and export as CSV, with fallbacks to HTML table parsing.
    """

    STATE = "CO"
    BASE_URL = "https://cdle.colorado.gov/employers/layoff-separations/layoff-warn-list"
    ALT_URLS = [
        "https://cdle.colorado.gov/warnlist",
        "https://cdle.colorado.gov/employers/layoff-separations",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Try primary URL
        try:
            resp = self._get(self.BASE_URL)
            parsed = self._scrape_page(resp, self.BASE_URL)
            if parsed:
                self.logger.info(f"CO main page: {len(parsed)} filings")
                results.extend(parsed)
        except Exception as e:
            self.logger.warning(f"CO main page failed: {e}")

        # Try alternative URLs if primary yielded no results
        if not results:
            for url in self.ALT_URLS:
                try:
                    resp = self._get(url)
                    parsed = self._scrape_page(resp, url)
                    if parsed:
                        self.logger.info(f"CO alt source {url}: {len(parsed)} filings")
                        results.extend(parsed)
                        break
                except Exception as e:
                    self.logger.warning(f"CO alt source {url} failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"CO scraper found {len(unique)} unique filings")
        return unique

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a CO WARN page, looking for Google Sheets embeds and HTML tables."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Strategy 1: Find embedded Google Sheets and export as CSV
        gsheet_results = self._extract_google_sheets(resp.text, soup)
        if gsheet_results:
            results.extend(gsheet_results)

        # Strategy 2: Look for direct download links (Excel/CSV)
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
                    parsed = self._parse_dataframe(df, full_url)
                    if parsed:
                        results.extend(parsed)
                except Exception as e:
                    self.logger.warning(f"CO download {full_url} failed: {e}")

        # Strategy 3: Parse HTML tables on the page
        if not results:
            try:
                tables = pd.read_html(resp.text)
                for df in tables:
                    if len(df) > 3:
                        parsed = self._parse_dataframe(df, page_url)
                        if parsed:
                            results.extend(parsed)
            except ValueError:
                pass

        # Strategy 4: BeautifulSoup fallback
        if not results:
            results.extend(self._parse_with_bs4(soup, page_url))

        return results

    def _extract_google_sheets(self, html: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Find embedded Google Sheets and export data as CSV."""
        results = []

        # Pattern 1: iframe src with docs.google.com/spreadsheets
        sheet_urls = set()

        for iframe in soup.find_all("iframe", src=True):
            src = iframe["src"]
            if "docs.google.com/spreadsheets" in src:
                sheet_urls.add(src)

        # Pattern 2: regex search in raw HTML for Google Sheets URLs
        gsheet_patterns = [
            r'https://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)',
            r'https://docs\.google\.com/spreadsheets/d/e/([a-zA-Z0-9_-]+)',
        ]
        for pattern in gsheet_patterns:
            matches = re.findall(pattern, html)
            for sheet_id in matches:
                # Construct the full URL based on pattern type
                if '/d/e/' in pattern:
                    sheet_urls.add(f"https://docs.google.com/spreadsheets/d/e/{sheet_id}/pub")
                else:
                    sheet_urls.add(f"https://docs.google.com/spreadsheets/d/{sheet_id}")

        # Try to export each sheet as CSV
        for sheet_url in sheet_urls:
            csv_urls = self._build_csv_export_urls(sheet_url)
            for csv_url in csv_urls:
                try:
                    resp = self._get(csv_url)
                    content = resp.content.decode("utf-8", errors="replace")

                    # Verify it looks like CSV data
                    if "," in content and len(content) > 50:
                        df = pd.read_csv(io.StringIO(content))
                        if len(df) > 1:
                            parsed = self._parse_dataframe(df, csv_url)
                            if parsed:
                                self.logger.info(f"CO Google Sheet: {len(parsed)} filings from {csv_url}")
                                results.extend(parsed)
                                break  # Got data from this sheet
                except Exception as e:
                    self.logger.warning(f"CO Google Sheet export {csv_url} failed: {e}")

        return results

    def _build_csv_export_urls(self, sheet_url: str) -> List[str]:
        """Build possible CSV export URLs from a Google Sheets URL."""
        urls = []

        # Extract sheet ID
        match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', sheet_url)
        if match:
            sheet_id = match.group(1)
            urls.append(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv")
            urls.append(f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv")

        # Handle /d/e/ (published) sheets
        match_pub = re.search(r'/spreadsheets/d/e/([a-zA-Z0-9_-]+)', sheet_url)
        if match_pub:
            pub_id = match_pub.group(1)
            urls.append(f"https://docs.google.com/spreadsheets/d/e/{pub_id}/pub?output=csv")

        # Also try appending /export?format=csv to the original URL
        base = sheet_url.rstrip("/")
        if "/export" not in base and "/pub" not in base:
            urls.append(f"{base}/export?format=csv")

        return urls

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
            if "company" in cl or "employer" in cl or "business" in cl or "organization" in cl:
                mapping["company"] = c
            elif "notice" in cl and "date" in cl:
                mapping["filing_date"] = c
            elif "warn" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "received" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "date" in cl and "filed" in cl:
                mapping.setdefault("filing_date", c)
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "closing" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "separation" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl or "region" in cl:
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
