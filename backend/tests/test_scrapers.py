"""Tests for scraper parsing logic."""

import sys
import os
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.scrapers.base_scraper import BaseScraper


class TestBaseScraper:
    def test_parse_date_various_formats(self):
        assert BaseScraper.parse_date("2023-01-15") == date(2023, 1, 15)
        assert BaseScraper.parse_date("01/15/2023") == date(2023, 1, 15)
        assert BaseScraper.parse_date("January 15, 2023") == date(2023, 1, 15)

    def test_parse_date_none(self):
        assert BaseScraper.parse_date(None) is None
        assert BaseScraper.parse_date("") is None
        assert BaseScraper.parse_date("   ") is None

    def test_parse_int_various_formats(self):
        assert BaseScraper.parse_int(500) == 500
        assert BaseScraper.parse_int("500") == 500
        assert BaseScraper.parse_int("1,500") == 1500
        assert BaseScraper.parse_int(500.0) == 500

    def test_parse_int_none(self):
        assert BaseScraper.parse_int(None) is None
        assert BaseScraper.parse_int("") is None
        assert BaseScraper.parse_int("nan") is None
        assert BaseScraper.parse_int(float("nan")) is None

    def test_parse_int_with_whitespace(self):
        assert BaseScraper.parse_int("  500  ") == 500
        assert BaseScraper.parse_int(" 1,234 ") == 1234
