"""Tests for address humanization."""
import pytest

from wswdy.address import humanize_address


@pytest.mark.parametrize("raw,expected", [
    # MPD's canonical "X - Y BLOCK OF Z DIRECTION" format
    ("1721 - 1799 BLOCK OF 19TH STREET NW", "1721–1799 Block of 19th Street NW"),
    ("3000 BLOCK OF 14 TH STREET NW", "3000 Block of 14 Th Street NW"),
    ("1500 14TH ST NW", "1500 14th St NW"),
    ("3000 CONNECTICUT AVENUE NW", "3000 Connecticut Avenue NW"),
    ("1100 23RD STREET NW", "1100 23rd Street NW"),
    # Crash-style addresses
    ("2521 MINNESOTA AVENUE SE", "2521 Minnesota Avenue SE"),
    ("981 KENILWORTH AVENUE NE", "981 Kenilworth Avenue NE"),
    ("CANAL ROAD NW", "Canal Road NW"),
    # Range hyphen variants
    ("1700 - 1799 P STREET NW", "1700–1799 P Street NW"),
])
def test_humanize_address_known_formats(raw, expected):
    assert humanize_address(raw) == expected


@pytest.mark.parametrize("d", ["NW", "NE", "SE", "SW", "N", "S", "E", "W"])
def test_humanize_address_directionals_stay_uppercase(d):
    assert humanize_address(f"100 MAIN ST {d}") == f"100 Main St {d}"


def test_humanize_address_lowercase_words_only_lowercase_mid_string():
    """First-word 'Of' would still be capitalized — that case shouldn't
    happen for real addresses, but the rule is title-case rules."""
    assert humanize_address("OF THE PEOPLE NW") == "Of the People NW"


def test_humanize_address_handles_none_and_empty():
    assert humanize_address(None) == ""
    assert humanize_address("") == ""
    assert humanize_address("   ") == ""


def test_humanize_address_ordinals_with_capitalize():
    """Internal: ordinals fall through to .capitalize() which happens to
    handle them correctly (number stays, suffix lowercased)."""
    assert humanize_address("100 1ST ST NW") == "100 1st St NW"
    assert humanize_address("100 2ND ST NW") == "100 2nd St NW"
    assert humanize_address("100 3RD ST NW") == "100 3rd St NW"
    assert humanize_address("100 21ST ST NW") == "100 21st St NW"
