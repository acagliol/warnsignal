import io
import re
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class PAScraper(BaseScraper):
    """Pennsylvania WARN Act scraper.

    PA publishes WARN notices through the Department of Labor & Industry.
    Data is often presented in HTML accordion/list format or as downloadable files.
    Multiple URL patterns are tried since PA has reorganized its site.
    """

    STATE = "PA"
    BASE_URL = "https://www.dli.pa.gov/Individuals/Workforce-Development/warn/Pages/default.aspx"

    ALT_URLS = [
        "https://www.pa.gov/agencies/dli/programs-services/workforce-development/warn-notices/",
        "https://www.dli.pa.gov/Individuals/Workforce-Development/warn/Pages/WARN-Notices.aspx",
        "https://www.dli.pa.gov/Individuals/Workforce-Development/warn/",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Strategy 1: Try the primary URL
        try:
            resp = self._get(self.BASE_URL)
            parsed = self._scrape_page(resp, self.BASE_URL)
            if parsed:
                self.logger.info(f"PA primary URL: {len(parsed)} filings")
                results.extend(parsed)
        except Exception as e:
            self.logger.warning(f"PA primary URL failed: {e}")

        # Strategy 2: Try alternate URLs
        if not results:
            for url in self.ALT_URLS:
                try:
                    resp = self._get(url)
                    parsed = self._scrape_page(resp, url)
                    if parsed:
                        self.logger.info(f"PA alt URL {url}: {len(parsed)} filings")
                        results.extend(parsed)
                        break
                except Exception as e:
                    self.logger.warning(f"PA alt URL {url} failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"PA scraper found {len(unique)} unique filings")
        return unique

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a PA WARN page for download links, tables, and accordion content."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Look for Excel/CSV download links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = self._resolve_url(href, page_url)
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
                        self.logger.info(f"PA download {full_url}: {len(parsed)} filings")
                except Exception as e:
                    self.logger.warning(f"PA download {full_url} failed: {e}")
                time.sleep(self.delay)

        # Try HTML tables with pandas
        if not results:
            try:
                tables = pd.read_html(resp.text)
                for df in tables:
                    if len(df) > 3:
                        parsed = self._parse_dataframe(df, page_url)
                        if parsed:
                            results.extend(parsed)
            except (ValueError, Exception):
                pass

        # Fallback: parse with BeautifulSoup (tables)
        if not results:
            results.extend(self._parse_with_bs4(resp.text, page_url))

        # Fallback: parse accordion/list format (PA-specific)
        if not results:
            results.extend(self._parse_accordion_listings(soup, page_url))

        # Try to find sub-page links (year-specific or paginated)
        if not results:
            results.extend(self._follow_sub_pages(soup, page_url))

        return results

    def _follow_sub_pages(self, soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
        """Follow links to year-specific or detailed WARN pages."""
        results = []
        visited = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True).lower()

            # Look for links mentioning WARN, notices, or years
            if ("warn" in text or "notice" in text) and any(
                str(y) in text or str(y) in href for y in range(2020, 2027)
            ):
                full_url = self._resolve_url(href, base_url)
                if full_url in visited or full_url == base_url:
                    continue
                visited.add(full_url)

                try:
                    resp = self._get(full_url)
                    # Try tables on the sub-page
                    try:
                        tables = pd.read_html(resp.text)
                        for df in tables:
                            if len(df) > 3:
                                parsed = self._parse_dataframe(df, full_url)
                                if parsed:
                                    results.extend(parsed)
                    except (ValueError, Exception):
                        pass

                    # Try BS4 on the sub-page
                    if not results:
                        results.extend(self._parse_with_bs4(resp.text, full_url))

                except Exception as e:
                    self.logger.warning(f"PA sub-page {full_url} failed: {e}")

                time.sleep(self.delay)
                if len(visited) >= 10:  # Safety limit
                    break

        return results

    def _parse_accordion_listings(self, soup: BeautifulSoup, source_url: str) -> List[Dict[str, Any]]:
        """Parse PA accordion/list-style WARN listings.

        PA sometimes uses SharePoint accordion controls or structured divs
        where each WARN notice is a collapsible item.
        """
        results = []

        # Look for accordion panels, collapsible sections, or structured divs
        panels = soup.find_all(["div", "section", "li"], class_=lambda c: c and any(
            kw in str(c).lower() for kw in ["accordion", "panel", "warn", "notice", "collapse", "item"]
        ))

        if not panels:
            # Try generic content containers
            panels = soup.find_all("div", class_=lambda c: c and "content" in str(c).lower())

        for panel in panels:
            text = panel.get_text(" ", strip=True)
            if len(text) < 20:
                continue

            record = self._extract_record_from_text(text, source_url)
            if record:
                results.append(record)

        return results

    def _extract_record_from_text(self, text: str, source_url: str) -> dict | None:
        """Try to extract a WARN record from unstructured text."""
        # Common patterns in PA WARN listings:
        # "Company Name | City, PA | Date | # Employees Affected"
        # or line-by-line within a panel

        lines = [l.strip() for l in re.split(r'[\n|;]', text) if l.strip()]
        if len(lines) < 2:
            return None

        company = None
        filing_date = None
        employees = None
        location = None

        for line in lines:
            # Skip very short or header-like lines
            if len(line) < 3:
                continue

            # Check for date
            parsed_date = self.parse_date(line)
            if parsed_date and not filing_date:
                filing_date = parsed_date
                continue

            # Check for employee count (look for number patterns)
            num_match = re.search(r'(\d[\d,]*)\s*(employee|worker|affected|layoff)', line, re.IGNORECASE)
            if num_match and not employees:
                employees = self.parse_int(num_match.group(1))
                continue

            # Check for PA location
            loc_match = re.search(r'([A-Za-z\s]+),?\s*PA\b', line)
            if loc_match and not location:
                location = loc_match.group(0).strip()
                continue

            # First substantial line is likely the company name
            if not company and len(line) > 3:
                company = line

        if company and filing_date:
            return {
                "company_name": company,
                "filing_date": filing_date,
                "layoff_date": None,
                "employees_affected": employees,
                "location": location,
                "source_url": source_url,
            }
        return None

    def _resolve_url(self, href: str, base_url: str) -> str:
        """Resolve a potentially relative URL against a base URL."""
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            # Extract domain from base_url
            from urllib.parse import urlparse
            parsed = urlparse(base_url)
            return f"{parsed.scheme}://{parsed.netloc}{href}"
        return f"{base_url.rsplit('/', 1)[0]}/{href}"

    def _parse_with_bs4(self, html: str, source_url: str) -> List[Dict[str, Any]]:
        """Fallback parser using BeautifulSoup for non-standard table layouts."""
        results = []
        soup = BeautifulSoup(html, "lxml")
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
            elif "dislocation" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl:
                mapping["location"] = c
        return mapping
