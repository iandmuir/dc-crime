"""Tests for MPD daily-report PDF parsing.

Uses mocked pdfplumber output to avoid committing real PDFs (the arrest
reports contain personally identifying info that shouldn't live in the
repo). The mock structure is the exact shape pdfplumber returns from
extract_tables() and extract_text(): tables are list[list[str|None]],
text is the rendered page string."""
from unittest.mock import MagicMock, patch

import pytest

from wswdy.pdf_reports import (
    _parse_dt,
    _table_to_dict,
    detect_pdf_kind,
    parse_arrest_pdf,
    parse_crime_pdf,
)


# ---------- helpers ----------

def test_parse_dt_handles_mpd_format():
    out = _parse_dt("May 2, 2026 3:19:11 AM")
    assert out is not None
    # ET (EDT in May) is UTC-4 → 3:19 AM ET == 7:19 AM UTC
    assert out.startswith("2026-05-02T07:19:11")


def test_parse_dt_handles_pm():
    out = _parse_dt("May 2, 2026 4:38:00 PM")
    # 4:38 PM EDT == 20:38 UTC
    assert out is not None
    assert out.startswith("2026-05-02T20:38:00")


def test_parse_dt_returns_none_for_unparseable():
    assert _parse_dt(None) is None
    assert _parse_dt("") is None
    assert _parse_dt("not a date") is None


def test_table_to_dict_normalizes_keys_and_collapses_whitespace():
    table = [
        ["PSA", "101"],
        ["CCN", "26058510"],
        ["RPT DATE", "May 2, 2026 3:19:11 AM"],
        ["BLOCK", "600 BLOCK OF K STREET NW"],
        # Multi-line address as pdfplumber emits it (newline inside cell)
        ["Arrest Location", "5028 BELT ROAD NW\nWASHINGTON, DC 20016\nUNITED STATES"],
        # Empty/None rows skipped
        [None, "noise"],
        ["", "ignored"],
    ]
    d = _table_to_dict(table)
    assert d["psa"] == "101"
    assert d["ccn"] == "26058510"
    assert d["rpt_date"] == "May 2, 2026 3:19:11 AM"
    assert d["block"] == "600 BLOCK OF K STREET NW"
    assert d["arrest_location"] == "5028 BELT ROAD NW WASHINGTON, DC 20016 UNITED STATES"
    assert "" not in d


# ---------- crime PDF ----------

def _crime_table(ccn: str, location: str, offense: str = "Theft",
                 method: str = "Theft (Second Degree)",
                 block: str = "600 BLOCK OF K STREET NW") -> list[list[str | None]]:
    return [
        ["PSA", "101"],
        ["CCN", ccn],
        ["RPT DATE", "May 2, 2026 3:19:11 AM"],
        ["OFFENSE", offense],
        ["METHOD", method],
        ["BLOCK", block],
        ["LOCATION", location],
        ["START DT", "May 2, 2026 2:01:00 AM"],
        ["END DT", "May 2, 2026 2:35:00 AM"],
    ]


def _mock_pdf(pages: list[dict]) -> MagicMock:
    """Build a context-manager-shaped mock that pdfplumber.open returns."""
    pdf = MagicMock()
    pdf.pages = []
    for page in pages:
        p = MagicMock()
        p.extract_text.return_value = page.get("text", "")
        p.extract_tables.return_value = page.get("tables", [])
        pdf.pages.append(p)
    cm = MagicMock()
    cm.__enter__.return_value = pdf
    cm.__exit__.return_value = False
    return cm


@patch("wswdy.pdf_reports.pdfplumber.open")
def test_parse_crime_pdf_extracts_records(mock_open):
    mock_open.return_value = _mock_pdf([{
        "text": "This report contains information about recent crimes "
                "reported in the 1D District.",
        "tables": [
            _crime_table("26058510", "Restaurant"),
            _crime_table("26058554", "Highway/ Road/ Alley/ Street/ Sidewalk",
                          offense="Robbery", method="Robbery"),
            _crime_table("26058888", "Church Synagogue Temple Mosque",
                          block="900 BLOCK OF MARYLAND AVENUE NE"),
        ],
    }])
    records = parse_crime_pdf("ignored.pdf")
    assert len(records) == 3
    assert [r.ccn for r in records] == ["26058510", "26058554", "26058888"]
    assert records[0].location == "Restaurant"
    assert records[1].offense == "Robbery"
    assert records[2].block == "900 BLOCK OF MARYLAND AVENUE NE"
    # All records inherit district from the page text
    assert all(r.district == "1D" for r in records)
    # rpt_date got converted to ISO UTC
    assert records[0].rpt_date.startswith("2026-05-02T")


@patch("wswdy.pdf_reports.pdfplumber.open")
def test_parse_crime_pdf_skips_legend_table(mock_open):
    """The crime PDF's last page has a legend table that doesn't have a CCN.
    Make sure we don't accidentally parse it as a crime record."""
    mock_open.return_value = _mock_pdf([{
        "text": "in the 3D District",
        "tables": [
            _crime_table("12345", "Restaurant"),
            # Looks like a key/value table but missing CCN — skipped
            [["KEY", ""], ["PSA - description", ""]],
        ],
    }])
    records = parse_crime_pdf("ignored.pdf")
    assert len(records) == 1


# ---------- arrest PDF ----------

def _arrest_table(arrest_number: str, last: str, first: str,
                  age: str = "30", felony: str = "MISDEMEANOR",
                  offense: str = "Simple Assault") -> list[list[str | None]]:
    return [
        ["Arrest Number#", arrest_number],
        ["Arrest Date & Time", "May 2, 2026 4:38:00 PM"],
        ["Arrest Location", "5028 BELT ROAD NW\nWASHINGTON, DC 20016\nUNITED STATES"],
        ["Arrest Location PSA", "202"],
        ["Offender Last Name", last],
        ["Offender First Name", first],
        ["Gender", "Male"],
        ["Age", age],
        ["Offense", offense],
        ["Felony/Misdemeanor", felony],
        ["Officer", "Doe 12345"],
    ]


@patch("wswdy.pdf_reports.pdfplumber.open")
def test_parse_arrest_pdf_extracts_records(mock_open):
    mock_open.return_value = _mock_pdf([{
        "text": "in the 2D District",
        "tables": [
            _arrest_table("022611844", "Slater", "Alexander", age="48",
                          felony="FELONY", offense="Threat To Kidnap Or Injure A Person"),
            _arrest_table("022611855", "Marzi", "Sadaf", age="25"),
        ],
    }])
    records = parse_arrest_pdf("ignored.pdf")
    assert len(records) == 2
    a, b = records
    assert a.arrest_number == "022611844"
    assert a.offender_first_name == "Alexander"
    assert a.offender_last_name == "Slater"
    assert a.age == 48
    assert a.felony_misdemeanor == "FELONY"
    assert a.district == "2D"
    # Address was multi-line in the raw cell; collapsed to single line
    assert "5028 BELT ROAD NW" in a.arrest_location
    assert "WASHINGTON, DC 20016" in a.arrest_location

    assert b.gender == "Male"  # both seeded as Male


@patch("wswdy.pdf_reports.pdfplumber.open")
def test_parse_arrest_pdf_handles_blank_felony_misdemeanor(mock_open):
    """Some arrests don't have a felony/misdemeanor classification — we
    should leave the field None rather than choking."""
    table = _arrest_table("X", "Doe", "Jane")
    table[9][1] = ""  # blank Felony/Misdemeanor
    mock_open.return_value = _mock_pdf([{"text": "", "tables": [table]}])
    records = parse_arrest_pdf("ignored.pdf")
    assert records[0].felony_misdemeanor is None


@patch("wswdy.pdf_reports.pdfplumber.open")
def test_parse_arrest_pdf_handles_unparseable_age(mock_open):
    table = _arrest_table("X", "Doe", "Jane", age="unknown")
    mock_open.return_value = _mock_pdf([{"text": "", "tables": [table]}])
    records = parse_arrest_pdf("ignored.pdf")
    assert records[0].age is None


# ---------- detect_pdf_kind ----------

@pytest.mark.parametrize("text,expected", [
    ("This report contains information about recent crimes reported", "crime"),
    ("This report contains information about recent arrests reported", "arrest"),
    ("Some other document entirely", None),
    ("", None),
])
@patch("wswdy.pdf_reports.pdfplumber.open")
def test_detect_pdf_kind(mock_open, text, expected):
    mock_open.return_value = _mock_pdf([{"text": text}])
    assert detect_pdf_kind("ignored.pdf") == expected


@patch("wswdy.pdf_reports.pdfplumber.open")
def test_detect_pdf_kind_returns_none_on_open_failure(mock_open):
    mock_open.side_effect = OSError("not a PDF")
    assert detect_pdf_kind("missing.pdf") is None
