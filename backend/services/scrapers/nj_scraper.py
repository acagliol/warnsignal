import io
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class NJScraper(BaseScraper):
    """New Jersey WARN Act scraper.

    NJ publishes WARN notices via the Department of Labor. Data may be
    available as Excel/PDF downloads or as HTML tables on the main page.
    Multiple URL patterns are tried since government sites change frequently.
    """

    STATE = "NJ"
    BASE_URL = "https://www.nj.gov/labor/employer-services/warn/"

    # Possible Excel/data download URLs (NJ changes these periodically)
    EXCEL_URLS = [
        "https://www.nj.gov/labor/employer-services/warn/WARN-Notice-Archive.xlsx",
        "https://www.nj.gov/labor/employer-services/warn/warn-notice-archive.xlsx",
        "https://www.nj.gov/labor/assets/PDFs/WARN/WARN-Notice-Archive.xlsx",
        "https://www.nj.gov/labor/employer-services/warn/WARN-Notices.xlsx",
        "https://www.nj.gov/labor/employer-services/warn/warn-notices.xlsx",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Strategy 1: Try direct Excel download URLs
        for url in self.EXCEL_URLS:
            try:
                resp = self._get(url)
                content_type = resp.headers.get("Content-Type", "")

                if "html" in content_type and len(resp.content) < 10000:
                    continue  # Skip error/redirect pages

                try:
                    df = pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
                except Exception:
                    try:
                        df = pd.read_excel(io.BytesIO(resp.content), engine="xlrd")
                    except Exception:
                        try:
                            df = pd.read_csv(io.BytesIO(resp.content))
                        except Exception:
                            self.logger.warning(f"NJ: could not parse {url}")
                            continue

                parsed = self._parse_dataframe(df, url)
                if parsed:
                    self.logger.info(f"NJ Excel {url}: {len(parsed)} filings")
                    results.extend(parsed)
                    break  # Got data from Excel, no need to try more URLs

            except Exception as e:
                self.logger.warning(f"NJ Excel {url} failed: {e}")

        # Strategy 2: Scrape the main WARN page for download links and HTML tables
        if not results:
            try:
                results = self._scrape_main_page()
            except Exception as e:
                self.logger.warning(f"NJ main page scrape failed: {e}")

        # Strategy 3: Try to find PDFs in the assets directory
        if not results:
            try:
                results = self._scrape_pdf_directory()
            except Exception as e:
                self.logger.warning(f"NJ PDF directory scrape failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"NJ scraper found {len(unique)} unique filings")
        return unique

    def _scrape_main_page(self) -> List[Dict[str, Any]]:
        """Scrape the main NJ WARN page for download links and HTML tables."""
        results = []
        resp = self._get(self.BASE_URL)
        soup = BeautifulSoup(resp.text, "lxml")

        # Find Excel/CSV download links on the page
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = href if href.startswith("http") else f"https://www.nj.gov{href}"
                if full_url not in self.EXCEL_URLS:
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
                            self.logger.info(f"NJ page link {full_url}: {len(parsed)} filings")
                    except Exception as e:
                        self.logger.warning(f"NJ page link {full_url} failed: {e}")

        # Try HTML tables on the page
        if not results:
            try:
                tables = pd.read_html(resp.text)
                for df in tables:
                    if len(df) > 3:
                        parsed = self._parse_dataframe(df, self.BASE_URL)
                        if parsed:
                            results.extend(parsed)
            except (ValueError, Exception):
                pass

        # Fallback: parse structured HTML content (NJ sometimes uses lists/divs)
        if not results:
            results = self._parse_html_listings(soup)

        return results

    def _scrape_pdf_directory(self) -> List[Dict[str, Any]]:
        """Try to find and parse Excel files from the NJ assets directory."""
        results = []
        pdf_base = "https://www.nj.gov/labor/assets/PDFs/WARN/"

        try:
            resp = self._get(pdf_base)
            soup = BeautifulSoup(resp.text, "lxml")

            for link in soup.find_all("a", href=True):
                href = link["href"]
                if any(ext in href.lower() for ext in [".xlsx", ".xls"]):
                    full_url = href if href.startswith("http") else f"{pdf_base}{href}"
                    try:
                        resp2 = self._get(full_url)
                        try:
                            df = pd.read_excel(io.BytesIO(resp2.content), engine="openpyxl")
                        except Exception:
                            df = pd.read_excel(io.BytesIO(resp2.content), engine="xlrd")
                        parsed = self._parse_dataframe(df, full_url)
                        if parsed:
                            results.extend(parsed)
                            self.logger.info(f"NJ PDF dir {full_url}: {len(parsed)} filings")
                    except Exception as e:
                        self.logger.warning(f"NJ PDF dir file {full_url} failed: {e}")
                    time.sleep(self.delay)
        except Exception as e:
            self.logger.warning(f"NJ PDF directory listing failed: {e}")

        return results

    def _parse_html_listings(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse NJ WARN listings from structured HTML (accordion, list, etc.)."""
        results = []

        # Look for common NJ patterns: accordion items, list items with WARN info
        for container in soup.find_all(["div", "li", "article"], class_=True):
            text = container.get_text(" ", strip=True)
            if len(text) < 20:
                continue

            # Try to extract company name and date from text blocks
            # NJ typically formats as: "Company Name - City, NJ - Date - # Employees"
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            if not lines:
                continue

            # Look for patterns with dates and numbers
            company = None
            filing_date = None
            employees = None
            location = None

            for line in lines:
                parsed_date = self.parse_date(line)
                if parsed_date and not filing_date:
                    filing_date = parsed_date
                    continue

                parsed_int = self.parse_int(line)
                if parsed_int and parsed_int > 0 and not employees:
                    employees = parsed_int
                    continue

                if not company and len(line) > 3:
                    company = line

            if company and filing_date:
                results.append({
                    "company_name": company,
                    "filing_date": filing_date,
                    "layoff_date": None,
                    "employees_affected": employees,
                    "location": location,
                    "source_url": self.BASE_URL,
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
            elif "city" in cl or "location" in cl or "county" in cl or "municipality" in cl:
                mapping["location"] = c
        return mapping
