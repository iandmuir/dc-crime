"""Parse MPD's daily LISTSERV PDF reports.

MPD emails out two daily PDF reports per police district (1D-7D):

  - **Crime report** — preliminary list of recent crimes. Same fields as
    the public ArcGIS feed *plus* a useful new LOCATION field
    ("Restaurant", "Residence/Home", "Church Synagogue Temple Mosque",
    "Highway/Road/Alley/Street/Sidewalk", etc).  Joined to the existing
    crimes table by CCN.

  - **Arrest report** — entirely new dataset. One row per arrest with
    full street address, arrestee first/last name, gender, age,
    offense, felony/misdemeanor, and arresting officer.

Both PDFs use the same layout: a series of two-column tables, one per
record, with field name in column 1 and value in column 2. Header line
identifies the district ("This report contains information about
recent crimes reported in the 1D District").

We use pdfplumber for table extraction since these are clean tabular
documents — no fancy text positioning needed.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pdfplumber

log = logging.getLogger(__name__)

# Captures the district code (1D-7D) from the report's title line, which
# always reads "recent crimes/arrests reported in the XD District". The
# tighter anchor protects against false positives elsewhere in the PDF —
# e.g. comparison stats, footer legend text, or a "see the 7D District
# for…" cross-reference — which would otherwise hijack district detection.
_DISTRICT_RE = re.compile(
    r"recent\s+(?:crimes|arrests)\s+reported\s+in\s+the\s+(\d+D)\s+District",
    re.IGNORECASE,
)


@dataclass
class CrimePdfRecord:
    """One crime extracted from an MPD daily crime PDF."""
    ccn: str                 # complaint number — joins to crimes.ccn
    psa: str | None          # police service area, e.g. "101"
    district: str | None     # police district, e.g. "1D"
    rpt_date: str | None     # ISO UTC string
    offense: str | None      # raw, e.g. "Theft"
    method: str | None       # subcategory, e.g. "Theft (Second Degree)"
    block: str | None
    location: str | None     # the new useful field, e.g. "Restaurant"
    start_dt: str | None
    end_dt: str | None


@dataclass
class ArrestPdfRecord:
    """One arrest extracted from an MPD daily arrest PDF."""
    arrest_number: str       # primary key, joins arrest records across runs
    district: str | None
    psa: str | None
    arrest_dt: str | None    # ISO UTC string
    arrest_location: str | None  # full multi-line address
    offender_first_name: str | None
    offender_last_name: str | None
    gender: str | None
    age: int | None
    offense: str | None
    felony_misdemeanor: str | None  # 'FELONY' / 'MISDEMEANOR' / None
    officer: str | None


# ---------- helpers --------------------------------------------------------

# MPD's date strings: "May 2, 2026 3:19:11 AM" or "May 2, 2026 4:38:00 PM".
# Parse via strptime; treat as ET local then convert to UTC. The PDFs
# don't include timezone info, but the events occur in DC (ET), so this
# is the right interpretation.
_DATE_FORMATS = (
    "%B %d, %Y %I:%M:%S %p",
    "%B %d, %Y %I:%M %p",  # some fields might omit seconds
)


def _parse_dt(s: str | None) -> str | None:
    """Parse MPD date string in ET, return ISO UTC string."""
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    for fmt in _DATE_FORMATS:
        try:
            naive = datetime.strptime(s, fmt)
            return naive.replace(tzinfo=et).astimezone(UTC).isoformat(timespec="seconds")
        except ValueError:
            continue
    log.debug("could not parse MPD date %r", s)
    return None


def _norm_key(label: str) -> str:
    """Canonicalize a table-label cell into a snake_case key."""
    return re.sub(r"[^a-z0-9]+", "_", label.strip().lower()).strip("_")


def _extract_district(text: str) -> str | None:
    m = _DISTRICT_RE.search(text or "")
    return m.group(1).upper() if m else None


def _table_to_dict(table: list[list[str | None]]) -> dict[str, str]:
    """Reduce a 2-column key/value table to a dict, joining wrapped lines.

    pdfplumber returns each cell as either a string or None. Multi-line
    cells (e.g. arrest addresses that span 3 lines) come through as a
    single string with newlines — we collapse those into one space-
    separated value.
    """
    out: dict[str, str] = {}
    for row in table:
        if len(row) < 2 or row[0] is None:
            continue
        key = _norm_key(row[0])
        if not key:
            continue
        val = (row[1] or "").strip()
        # Collapse internal whitespace incl. newlines from wrapped cells
        val = re.sub(r"\s+", " ", val)
        if val:
            out[key] = val
    return out


# ---------- crime PDF ------------------------------------------------------

def parse_crime_pdf(path: Path | str) -> list[CrimePdfRecord]:
    """Extract crime records from an MPD daily crime PDF."""
    path = Path(path)
    records: list[CrimePdfRecord] = []
    district: str | None = None

    with pdfplumber.open(path) as pdf:
        # District is in the header on page 1; capture it once.
        for page in pdf.pages:
            if district is None:
                district = _extract_district(page.extract_text() or "")
            for table in page.extract_tables() or []:
                rec = _table_to_dict(table)
                ccn = rec.get("ccn")
                if not ccn or "offense" not in rec:
                    continue  # legend / spurious table — skip
                records.append(CrimePdfRecord(
                    ccn=ccn,
                    psa=rec.get("psa"),
                    district=district,
                    rpt_date=_parse_dt(rec.get("rpt_date")),
                    offense=rec.get("offense"),
                    method=rec.get("method"),
                    block=rec.get("block"),
                    location=rec.get("location"),
                    start_dt=_parse_dt(rec.get("start_dt")),
                    end_dt=_parse_dt(rec.get("end_dt")),
                ))
    return records


# ---------- arrest PDF -----------------------------------------------------

def parse_arrest_pdf(path: Path | str) -> list[ArrestPdfRecord]:
    """Extract arrest records from an MPD daily arrest PDF."""
    path = Path(path)
    records: list[ArrestPdfRecord] = []
    district: str | None = None

    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            if district is None:
                district = _extract_district(page.extract_text() or "")
            for table in page.extract_tables() or []:
                rec = _table_to_dict(table)
                arrest_number = rec.get("arrest_number")
                if not arrest_number:
                    continue

                age_raw = rec.get("age")
                try:
                    age = int(age_raw) if age_raw else None
                except (TypeError, ValueError):
                    age = None

                records.append(ArrestPdfRecord(
                    arrest_number=arrest_number,
                    district=district,
                    psa=rec.get("arrest_location_psa"),
                    arrest_dt=_parse_dt(rec.get("arrest_date_time")),
                    arrest_location=rec.get("arrest_location"),
                    offender_first_name=rec.get("offender_first_name"),
                    offender_last_name=rec.get("offender_last_name"),
                    gender=rec.get("gender"),
                    age=age,
                    offense=rec.get("offense"),
                    felony_misdemeanor=rec.get("felony_misdemeanor") or None,
                    officer=rec.get("officer"),
                ))
    return records


# ---------- detection helper ----------------------------------------------

def detect_pdf_kind(path: Path | str) -> str | None:
    """Return 'crime', 'arrest', or None based on the report header.

    Useful when the email source dumps PDFs into a single inbox folder
    and we need to route each to the right parser.
    """
    path = Path(path)
    try:
        with pdfplumber.open(path) as pdf:
            text = (pdf.pages[0].extract_text() or "") if pdf.pages else ""
    except Exception:
        return None
    t = text.lower()
    if "recent crimes reported" in t:
        return "crime"
    if "recent arrests reported" in t:
        return "arrest"
    return None
