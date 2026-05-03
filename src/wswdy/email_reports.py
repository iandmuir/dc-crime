"""Parse MPD's daily LISTSERV email reports (plain-text body).

Background
----------
MPD sends two kinds of daily emails per police district:

    Subject: "MPD: Preliminary Crime Report for 2D"
    Subject: "MPD: Preliminary Arrest Report for 2D"

The plain-text body of each email contains the same field/value pairs
that show up in the PDF attachment, but UNLIKE the PDF, the email body
always corresponds to the district named in the subject. (The PDF
attachment is a separate, semi-randomly-rotating file that doesn't
reliably match.) So the email body is the source of truth.

Body structure (Crime example)
------------------------------
::

    This report contains information about recent crimes reported in the 2D
     District.
    PSA 202
    CCN 26058921
    RPT DATE May 2, 2026 11:03:01 PM
    OFFENSE Theft
    METHOD Theft (Second Degree)
    BLOCK 4200 BLOCK OF ALBEMARLE STREET NW
    LOCATION Church Synagogue Temple Mosque
    START DT May 2, 2026 2:11:00 AM
    END DT May 2, 2026 5:01:00 AM
    PSA 203
    CCN 26058566
    ...
    KEY:
    PSA - The Police District ...

Each record begins with ``PSA <psa>`` (crimes) or ``Arrest Number# <id>``
(arrests), followed by labelled lines until the next record or the
``KEY:`` legend footer.

We deliberately keep these dataclasses **structurally identical** to the
PDF parser's records (CrimePdfRecord, ArrestPdfRecord) so the existing
ingest job can persist either source through the same repo functions.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)


# MPD's date strings: "May 2, 2026 11:03:01 PM" or "May 2, 2026 4:38 PM".
# The emails don't include timezone info, but the events occur in DC (ET),
# so we parse as ET and convert to UTC for storage.
_DATE_FORMATS = (
    "%B %d, %Y %I:%M:%S %p",
    "%B %d, %Y %I:%M %p",  # some fields might omit seconds
)


def _parse_dt(s: str | None) -> str | None:
    """Parse an MPD date string in ET, return ISO UTC string."""
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    et = ZoneInfo("America/New_York")
    for fmt in _DATE_FORMATS:
        try:
            naive = datetime.strptime(s, fmt)
            return naive.replace(tzinfo=et).astimezone(UTC).isoformat(timespec="seconds")
        except ValueError:
            continue
    log.debug("could not parse MPD date %r", s)
    return None


# Subject patterns ----------------------------------------------------------
# "MPD: Preliminary Crime Report for 2D" or "MPD: Preliminary Arrest
# Report for 2D" (with optional "Fwd: " prefix when subscribers forward
# the email manually for testing).
_SUBJECT_KIND_RE = re.compile(r"\b(crime|arrest)\s+report\b", re.IGNORECASE)
_SUBJECT_DISTRICT_RE = re.compile(r"\b(\d+)D\b")

# Same anchor as pdf_reports — finds the title sentence in the body.
_BODY_DISTRICT_RE = re.compile(
    r"recent\s+(?:crimes|arrests)\s+reported\s+in\s+the\s+(\d+D)\s+District",
    re.IGNORECASE,
)

# "KEY:" on its own line marks the legend footer — stop parsing there.
_KEY_FOOTER = "KEY:"


# Field labels (in body) keyed by canonical field name. Order matters for
# the multi-word labels: longer labels must be checked before shorter
# prefixes (e.g. "RPT DATE" before "RPT").
_CRIME_FIELDS = (
    ("psa",       "PSA"),
    ("ccn",       "CCN"),
    ("rpt_date",  "RPT DATE"),
    ("offense",   "OFFENSE"),
    ("method",    "METHOD"),
    ("block",     "BLOCK"),
    ("location",  "LOCATION"),
    ("start_dt",  "START DT"),
    ("end_dt",    "END DT"),
)

_ARREST_FIELDS = (
    ("arrest_number",     "Arrest Number#"),
    ("arrest_dt",         "Arrest Date & Time"),
    ("arrest_location",   "Arrest Location"),
    ("psa",               "Arrest Location PSA"),
    ("offender_last_name",  "Offender Last Name"),
    ("offender_first_name", "Offender First Name"),
    ("gender",            "Gender"),
    ("age",               "Age"),
    ("offense",           "Offense"),
    ("felony_misdemeanor","Felony/Misdemeanor"),
    ("officer",           "Officer"),
)


# ---------- record dataclasses ---------------------------------------------

@dataclass
class CrimeEmailRecord:
    ccn: str
    psa: str | None
    district: str | None
    rpt_date: str | None       # ISO UTC
    offense: str | None
    method: str | None
    block: str | None
    location: str | None
    start_dt: str | None
    end_dt: str | None


@dataclass
class ArrestEmailRecord:
    arrest_number: str
    district: str | None
    psa: str | None
    arrest_dt: str | None      # ISO UTC
    arrest_location: str | None
    offender_first_name: str | None
    offender_last_name: str | None
    gender: str | None
    age: int | None
    offense: str | None
    felony_misdemeanor: str | None
    officer: str | None


# ---------- subject helpers ------------------------------------------------

def detect_kind_from_subject(subject: str) -> str | None:
    """Return ``'crime'`` / ``'arrest'`` / ``None`` from the subject line."""
    m = _SUBJECT_KIND_RE.search(subject or "")
    if not m:
        return None
    return m.group(1).lower()


def extract_district(subject: str | None, body_text: str | None = None) -> str | None:
    """Pull the police district code (e.g. ``'2D'``) from the subject first,
    falling back to the body title line. Subject is preferred because it's
    canonical — set by MPD on send, not subject to body-formatting drift."""
    if subject:
        m = _SUBJECT_DISTRICT_RE.search(subject)
        if m:
            return f"{m.group(1)}D".upper()
    if body_text:
        m = _BODY_DISTRICT_RE.search(body_text)
        if m:
            return m.group(1).upper()
    return None


# ---------- block iterator -------------------------------------------------

def _iter_record_blocks(
    text: str, *, fields: tuple[tuple[str, str], ...], start_field: str,
) -> list[dict[str, str]]:
    """Walk the body line-by-line, splitting it into records.

    ``fields`` is the kind-specific list of ``(canonical_key, label)``
    pairs to recognize (passed by the caller — *only* crime fields when
    parsing a crime email, *only* arrest fields when parsing an arrest
    email). This kind-awareness matters because some labels collide
    case-insensitively across the two formats — most importantly the
    crime ``OFFENSE`` and arrest ``Offense`` labels — and a single
    shared scanner would silently drop one or the other.

    A record begins on any line whose first labelled field equals
    ``start_field`` (e.g. ``PSA`` for crime, ``Arrest Number#`` for
    arrest). Returns a list of ``{canonical_key: value}`` dicts. Stops
    at ``KEY:`` or end-of-text.
    """
    blocks: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    started = False
    start_key = next(k for k, label in fields if label == start_field)

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.upper() == _KEY_FOOTER:
            break

        # Skip everything before the first record (forwarded headers,
        # title line, image alt-text, etc.).
        if not started:
            if line.startswith(start_field):
                started = True
            else:
                continue

        # New-record detection: a line that starts with the
        # start_field label opens a new block.
        if line.startswith(start_field) and current and start_key in current:
            blocks.append(current)
            current = {}
        if current is None:
            current = {}

        key, value = _split_label(line, fields=fields)
        if key:
            current[key] = value
        # Lines that don't match any known label are ignored.

    if current:
        blocks.append(current)
    return blocks


def _split_label(
    line: str, *, fields: tuple[tuple[str, str], ...],
) -> tuple[str | None, str]:
    """Return ``(canonical_key, value)`` for a body line.

    Only checks ``fields`` (the kind-specific list passed by the
    caller). When multiple labels could match a line (e.g. ``PSA`` and
    ``Arrest Location PSA`` both prefix ``Arrest Location PSA 202``),
    the longest label wins so the more specific one always takes
    precedence over a shared prefix.
    """
    line_norm = line.strip()
    line_upper = line_norm.upper()
    candidates: list[tuple[str, str]] = []
    for key, label in fields:
        label_upper = label.upper()
        if line_upper.startswith(label_upper + " ") or line_upper == label_upper:
            candidates.append((key, label))
    if not candidates:
        return None, ""
    key, label = max(candidates, key=lambda kl: len(kl[1]))
    value = line_norm[len(label):].strip()
    return key, value


# ---------- public parsers -------------------------------------------------

def parse_crime_email(*, subject: str, text_body: str) -> list[CrimeEmailRecord]:
    """Parse the plain-text body of an MPD Preliminary Crime Report email."""
    district = extract_district(subject, text_body)
    blocks = _iter_record_blocks(
        text_body, fields=_CRIME_FIELDS, start_field="PSA",
    )

    records: list[CrimeEmailRecord] = []
    for rec in blocks:
        ccn = rec.get("ccn")
        if not ccn:
            continue
        records.append(CrimeEmailRecord(
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


def parse_arrest_email(*, subject: str, text_body: str) -> list[ArrestEmailRecord]:
    """Parse the plain-text body of an MPD Preliminary Arrest Report email."""
    district = extract_district(subject, text_body)
    blocks = _iter_record_blocks(
        text_body, fields=_ARREST_FIELDS, start_field="Arrest Number#",
    )

    records: list[ArrestEmailRecord] = []
    for rec in blocks:
        arrest_number = rec.get("arrest_number")
        if not arrest_number:
            continue

        age_raw = rec.get("age")
        try:
            age = int(age_raw) if age_raw else None
        except (TypeError, ValueError):
            age = None

        records.append(ArrestEmailRecord(
            arrest_number=arrest_number,
            district=district,
            psa=rec.get("psa"),
            arrest_dt=_parse_dt(rec.get("arrest_dt")),
            arrest_location=rec.get("arrest_location"),
            offender_first_name=rec.get("offender_first_name"),
            offender_last_name=rec.get("offender_last_name"),
            gender=rec.get("gender"),
            age=age,
            offense=rec.get("offense"),
            felony_misdemeanor=(rec.get("felony_misdemeanor") or None),
            officer=rec.get("officer"),
        ))
    return records
