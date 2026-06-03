"""Tests for entity resolution."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.entity_resolution.sp1500 import normalize_company_name


class TestNormalization:
    def test_strips_inc(self):
        assert normalize_company_name("Bed Bath & Beyond Inc") == "BED BATH BEYOND"

    def test_strips_corp(self):
        assert normalize_company_name("Rite Aid Corporation") == "RITE AID"

    def test_strips_llc(self):
        assert normalize_company_name("Joe's Pizza LLC") == "JOES PIZZA"

    def test_strips_holdco(self):
        assert normalize_company_name("Party City Holdco Inc") == "PARTY CITY"

    def test_uppercase(self):
        result = normalize_company_name("apple inc")
        assert result == "APPLE"

    def test_strips_punctuation(self):
        result = normalize_company_name("Johnson & Johnson")
        assert "JOHNSON" in result

    def test_handles_empty(self):
        result = normalize_company_name("")
        assert result == ""

    def test_handles_only_suffix(self):
        result = normalize_company_name("Inc")
        assert result == ""

    def test_collapses_whitespace(self):
        result = normalize_company_name("  SOME   COMPANY   INC  ")
        assert "  " not in result


class TestEntityResolution:
    """Integration tests that require the SP1500 index to be loaded."""

    def test_known_company_normalizes_correctly(self):
        # These should produce clean names suitable for fuzzy matching
        names = {
            "BED BATH & BEYOND INC": "BED BATH BEYOND",
            "RITE AID CORPORATION": "RITE AID",
            "PARTY CITY HOLDCO INC": "PARTY CITY",
            "REVLON INC": "REVLON",
        }
        for raw, expected in names.items():
            assert normalize_company_name(raw) == expected, f"Failed for {raw}"

    def test_private_company_name_normalizes(self):
        result = normalize_company_name("JOE'S LOCAL PIZZA LLC")
        assert result == "JOES LOCAL PIZZA"
