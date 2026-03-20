import io
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class MOScraper(BaseScraper):
    """Missouri WARN Act scraper.

    MO publishes WARN notices through the Department of Higher Education
    and Workforce Development. Data may be available as downloadable reports
    or HTML table listings.
    """

    STATE = "MO"
    BASE_URL = "https://jobs.mo.gov/warn-notices"
    ALT_URLS = [
        "https://labor.mo.gov/warn-notices",
        "https://jobs.mo.gov/content/warn-notices",
        "https://labor.mo.gov/DES/warn",
        "https://jobs.mo.gov/warn",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Try primary URL first
        try:
            resp = self._get(self.BASE_URL)
            parsed = self._scrape_page(resp, self.BASE_URL)
            if parsed:
                self.logger.info(f"MO primary URL: {len(parsed)} filings")
                results.extend(parsed)
        except Exception as e:
            self.logger.warning(f"MO primary URL failed: {e}")

        # Try alternate URLs if primary yielded nothing
        if not results:
            for url in self.ALT_URLS:
                try:
                    resp = self._get(url)
                    parsed = self._scrape_page(resp, url)
                    if parsed:
                        self.logger.info(f"MO alt source {url}: {len(parsed)} filings")
                        results.extend(parsed)
                        break
                except Exception as e:
                    self.logger.warning(f"MO alt source {url} failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"MO scraper found {len(unique)} unique filings")
        return unique

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a single MO WARN page for download links and HTML tables."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Find Excel/CSV download links
        download_links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()

            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv", ".pdf"]):
                if not href.lower().endswith(".pdf"):  # Skip PDF links
                    full_url = href if href.startswith("http") else self._resolve_url(page_url, href)
                    download_links.append(full_url)
            elif "warn" in text and ("download" in text or "report" in text or "list" in text):
                full_url = href if href.startswith("http") else self._resolve_url(page_url, href)
                download_links.append(full_url)

        self.logger.info(f"MO: found {len(download_links)} potential download links on {page_url}")

        for url in download_links:
            try:
                resp2 = self._get(url)
                content_type = resp2.headers.get("Content-Type", "")

                if "html" in content_type and len(resp2.content) < 10000:
                    continue  # Skip error pages

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
                    self.logger.info(f"MO: {url} -> {len(parsed)} filings")
                    results.extend(parsed)

                time.sleep(self.delay)
            except Exception as e:
                self.logger.warning(f"MO download {url} failed: {e}")

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

        # Check for paginated content
        if results:
            results.extend(self._scrape_pagination(soup, page_url))

        return results

    def _scrape_pagination(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
        """Follow pagination links if present."""
        results = []
        page = 2

        while page <= 50:  # Safety limit
            next_link = soup.find("a", href=lambda h: h and f"page={page}" in h if h else False)
            if not next_link:
                next_link = soup.find("a", string=lambda t: t and "next" in t.lower() if t else False)
            if not next_link:
                break

            href = next_link["href"]
            next_url = href if href.startswith("http") else self._resolve_url(base_url, href)

            try:
                resp = self._get(next_url)
                soup = BeautifulSoup(resp.text, "lxml")

                try:
                    tables = pd.read_html(resp.text)
                    page_found = False
                    for df in tables:
                        if len(df) > 0:
                            parsed = self._parse_dataframe(df, next_url)
                            if parsed:
                                results.extend(parsed)
                                page_found = True
                    if not page_found:
                        break
                except ValueError:
                    break

                page += 1
                time.sleep(self.delay)
            except Exception as e:
                self.logger.warning(f"MO pagination page {page} failed: {e}")
                break

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
            if "company" in cl or "employer" in cl or "business" in cl or "organization" in cl:
                mapping["company"] = c
            elif "notice" in cl and "date" in cl:
                mapping["filing_date"] = c
            elif "warn" in cl and "date" in cl:
                mapping.setdefault("filing_date", c)
            elif "received" in cl and "date" in cl:
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
