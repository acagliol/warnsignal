import io
import json
import time
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
from services.scrapers.base_scraper import BaseScraper


class GAScraper(BaseScraper):
    """Georgia WARN Act scraper.

    GA publishes WARN notices through the Technical College System of Georgia (TCSG).
    The data may be served via an AJAX/JSON endpoint or as HTML tables.
    Multiple URL patterns are tried since government sites change frequently.
    """

    STATE = "GA"
    BASE_URL = "https://www.tcsg.edu/warn-public-view/"

    ALT_URLS = [
        "https://www.georgia.org/warn-notices",
        "https://www.tcsg.edu/warn/",
        "https://www.tcsg.edu/workforce-solutions/warn-notices/",
    ]

    # TCSG may use WordPress admin-ajax for data tables
    AJAX_URLS = [
        "https://www.tcsg.edu/wp-admin/admin-ajax.php",
    ]

    def scrape(self) -> List[Dict[str, Any]]:
        results = []

        # Strategy 1: Try the primary URL and look for AJAX data
        try:
            resp = self._get(self.BASE_URL)
            # Check for DataTables AJAX endpoint in the page source
            ajax_results = self._try_ajax_endpoint(resp.text)
            if ajax_results:
                self.logger.info(f"GA AJAX endpoint: {len(ajax_results)} filings")
                results.extend(ajax_results)
            else:
                # Parse the page directly
                parsed = self._scrape_page(resp, self.BASE_URL)
                if parsed:
                    self.logger.info(f"GA primary URL: {len(parsed)} filings")
                    results.extend(parsed)
        except Exception as e:
            self.logger.warning(f"GA primary URL failed: {e}")

        # Strategy 2: Try alternate URLs
        if not results:
            for url in self.ALT_URLS:
                try:
                    resp = self._get(url)
                    parsed = self._scrape_page(resp, url)
                    if parsed:
                        self.logger.info(f"GA alt URL {url}: {len(parsed)} filings")
                        results.extend(parsed)
                        break
                except Exception as e:
                    self.logger.warning(f"GA alt URL {url} failed: {e}")

        # Deduplicate by company + filing date
        seen = set()
        unique = []
        for r in results:
            key = (r.get("company_name", ""), r.get("filing_date"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        self.logger.info(f"GA scraper found {len(unique)} unique filings")
        return unique

    def _try_ajax_endpoint(self, page_html: str) -> List[Dict[str, Any]]:
        """Try to find and call an AJAX endpoint for WARN data.

        TCSG may use WordPress DataTables plugin that loads data via admin-ajax.php.
        """
        results = []

        # Look for AJAX URL patterns in the page source
        import re
        ajax_patterns = [
            r'ajax["\s]*:\s*["\']([^"\']+)["\']',
            r'url["\s]*:\s*["\']([^"\']*admin-ajax[^"\']*)["\']',
            r'data-source=["\']([^"\']+)["\']',
            r'["\']ajaxurl["\']\s*:\s*["\']([^"\']+)["\']',
        ]

        ajax_url = None
        for pattern in ajax_patterns:
            match = re.search(pattern, page_html)
            if match:
                ajax_url = match.group(1)
                break

        if not ajax_url:
            # Try default WordPress AJAX URL
            ajax_url = "https://www.tcsg.edu/wp-admin/admin-ajax.php"

        # Try common AJAX action names for WARN data
        actions = [
            "get_warn_notices",
            "warn_data",
            "get_datatable",
            "wp_ajax_get_warn",
        ]

        for action in actions:
            try:
                resp = self.session.post(
                    ajax_url,
                    data={"action": action, "length": 1000, "start": 0},
                    timeout=30,
                    headers={"X-Requested-With": "XMLHttpRequest"},
                )
                self._rate_limit()

                if resp.status_code != 200:
                    continue

                try:
                    data = resp.json()
                except (json.JSONDecodeError, ValueError):
                    continue

                # Handle DataTables server-side response format
                if isinstance(data, dict) and "data" in data:
                    records = data["data"]
                elif isinstance(data, list):
                    records = data
                else:
                    continue

                for record in records:
                    parsed = self._parse_ajax_record(record)
                    if parsed:
                        results.append(parsed)

                if results:
                    self.logger.info(f"GA AJAX action '{action}': {len(results)} filings")
                    break

            except Exception as e:
                self.logger.debug(f"GA AJAX action '{action}' failed: {e}")

        return results

    def _parse_ajax_record(self, record: Any) -> dict | None:
        """Parse a single record from AJAX JSON response."""
        if isinstance(record, list):
            # DataTables array format: [company, date, employees, location, ...]
            if len(record) < 2:
                return None

            # Strip HTML tags from cell values
            import re
            clean = [re.sub(r'<[^>]+>', '', str(v)).strip() for v in record]

            company = clean[0] if clean[0] and clean[0].lower() != "nan" else None
            if not company:
                return None

            filing_date = None
            employees = None
            location = None

            for val in clean[1:]:
                if not filing_date:
                    filing_date = self.parse_date(val)
                    if filing_date:
                        continue
                if not employees:
                    employees = self.parse_int(val)
                    if employees and employees > 0:
                        continue
                if not location and val and len(val) > 2:
                    location = val

            return {
                "company_name": company,
                "filing_date": filing_date,
                "layoff_date": None,
                "employees_affected": employees,
                "location": location,
                "source_url": self.BASE_URL,
            }

        elif isinstance(record, dict):
            # Dict format: look for known keys
            col_map = {}
            for key in record:
                kl = str(key).lower()
                if "company" in kl or "employer" in kl or "business" in kl:
                    col_map["company"] = key
                elif "notice" in kl and "date" in kl:
                    col_map["filing_date"] = key
                elif "warn" in kl and "date" in kl:
                    col_map.setdefault("filing_date", key)
                elif "date" in kl and "filing_date" not in col_map:
                    col_map.setdefault("filing_date", key)
                elif "effective" in kl or ("layoff" in kl and "date" in kl):
                    col_map["layoff_date"] = key
                elif "employee" in kl or "worker" in kl or "affected" in kl or "number" in kl:
                    col_map.setdefault("employees", key)
                elif "city" in kl or "location" in kl or "county" in kl:
                    col_map.setdefault("location", key)

            company_key = col_map.get("company")
            if not company_key:
                return None

            import re
            company = re.sub(r'<[^>]+>', '', str(record.get(company_key, ""))).strip()
            if not company or company.lower() == "nan":
                return None

            return {
                "company_name": company,
                "filing_date": self.parse_date(record.get(col_map.get("filing_date", ""))),
                "layoff_date": self.parse_date(record.get(col_map.get("layoff_date", ""))),
                "employees_affected": self.parse_int(record.get(col_map.get("employees", ""))),
                "location": re.sub(r'<[^>]+>', '', str(record.get(col_map.get("location", ""), ""))).strip() or None,
                "source_url": self.BASE_URL,
            }

        return None

    def _scrape_page(self, resp, page_url: str) -> List[Dict[str, Any]]:
        """Scrape a GA WARN page for download links and HTML tables."""
        results = []
        soup = BeautifulSoup(resp.text, "lxml")

        # Look for Excel/CSV download links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(ext in href.lower() for ext in [".xlsx", ".xls", ".csv"]):
                full_url = href if href.startswith("http") else f"https://www.tcsg.edu{href}"
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
                        self.logger.info(f"GA download {full_url}: {len(parsed)} filings")
                except Exception as e:
                    self.logger.warning(f"GA download {full_url} failed: {e}")
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

        # Fallback: parse with BeautifulSoup
        if not results:
            results = self._parse_with_bs4(resp.text, page_url)

        return results

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
            elif "date" in cl and "filing_date" not in mapping:
                mapping.setdefault("filing_date", c)
            elif "effective" in cl or ("layoff" in cl and "date" in cl):
                mapping["layoff_date"] = c
            elif "closure" in cl and "date" in cl:
                mapping.setdefault("layoff_date", c)
            elif "employee" in cl or "worker" in cl or "affected" in cl or "number" in cl:
                if "employees" not in mapping:
                    mapping["employees"] = c
            elif "city" in cl or "location" in cl or "county" in cl:
                mapping["location"] = c
        return mapping
