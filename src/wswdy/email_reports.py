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


# MPD's date strings come in two month-name styles:
#
#   "May 2, 2026 4:38:00 PM"         (or "May 2, 2026 4:38 PM")
#   "Apr 30, 2026 7:00:00 AM"        (or "Apr 30, 2026 7:00 AM")
#
# The 3-letter abbreviated form (Apr/May/Jun/...) is what MPD's templates
# generally produce. We list the full-name variants too as a defensive
# fallback in case a future template change goes back to spelled-out
# months. The emails don't carry timezone info, so we parse as ET (where
# the events happen) and convert to UTC for storage.
_DATE_FORMATS = (
    "%b %d, %Y %I:%M:%S %p",
    "%b %d, %Y %I:%M %p",
    "%B %d, %Y %I:%M:%S %p",
    "%B %d, %Y %I:%M %p",
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

# "KEY:" on its own line marks the legend footer of crime reports — stop
# parsing there. Arrest reports don't have a KEY footer at all; they
# embed "Reminder:", "Disclaimer:", and "Please Note:" prose paragraphs
# IN THE MIDDLE of records (between the day's first three arrests and
# the rest). Those mid-email paragraphs are NOT terminators — we just
# silently ignore lines that don't match a known field label, and rely
# on _CONTINUABLE_FIELDS below to keep the continuation-line logic
# from gluing them onto the previous record.
_FOOTER_RE = re.compile(r"^KEY\s*[:\-]", re.IGNORECASE)


# Only these fields ever wrap onto multiple lines in MPD's emails:
#
#   arrest_location: street / city+state+zip / country (always 3 lines)
#   block:           occasionally wraps for very long block names
#   offense:         long parenthetical offenses wrap mid-sentence
#                    ("...Place of\nBusiness)")
#   method:          subcategory text occasionally wraps
#
# Other fields (officer, age, gender, dates, ...) are always single-line.
# Restricting continuation to this set is what stops the "Reminder:" /
# "Disclaimer:" boilerplate paragraphs from being glued onto the last
# record's officer.
_CONTINUABLE_FIELDS = frozenset({"arrest_location", "block", "offense", "method"})


# Strip the obvious-context tail ("WASHINGTON, DC <ZIP> UNITED STATES")
# from arrest addresses, keeping just street + ZIP. Every address in
# the feed is in DC by construction, so the city/state/country bytes are
# pure noise — they push the popup-relevant info off the screen and
# render as "Washington, Dc" once humanize_address title-cases them.
# Falls back to the raw string if the tail isn't present (e.g. for
# bare intersection addresses like "14TH STREET NW & K STREET NW").
_ADDR_TAIL_RE = re.compile(
    r"^(?P<street>.+?)\s+WASHINGTON,?\s+D\.?C\.?\s+(?P<zip>\d{5})"
    r"(?:\s+UNITED\s+STATES)?\s*$",
    re.IGNORECASE,
)
# Defensive cleanup for partial / malformed cases. We've seen all of:
#
#   "INDEPENDENCE AVENUE SW UNITED STATES"
#   "200 K ST NW WASHINGTON, DC"           (no zip)
#   "1101 NEW YORK AVE NW WASHINGTON, 2 DC" (typo in MPD's data)
#
# So strip everything from "WASHINGTON" onward when present, plus any
# trailing "UNITED STATES" suffix on its own.
_ADDR_TRAILING_NOISE_RE = re.compile(
    r"\s+(WASHINGTON\b.*|UNITED\s+STATES)\s*$",
    re.IGNORECASE,
)


def clean_arrest_location(addr: str | None) -> str | None:
    """Trim the city/state/country tail from an arrest address.

    Returns ``"<street>, <zip>"`` when the standard tail is present,
    otherwise removes only the trailing "WASHINGTON, DC" / "UNITED STATES"
    suffix. Intersection-style addresses (which often lack a zip) come
    through unchanged.
    """
    if not addr:
        return addr
    m = _ADDR_TAIL_RE.match(addr.strip())
    if m:
        street = m.group("street").rstrip(", ").strip()
        return f"{street}, {m.group('zip')}"
    cleaned = addr.strip()
    while True:
        new = _ADDR_TRAILING_NOISE_RE.sub("", cleaned).strip().rstrip(",")
        if new == cleaned:
            break
        cleaned = new
    return cleaned


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

def _normalize_body(text: str, fields: tuple[tuple[str, str], ...]) -> str:
    """Split inline label sequences onto separate lines.

    MPD's crime emails (as of May 2026) sometimes pack an entire record
    onto a single line::

        PSA 603 CCN 26059195 RPT DATE May 3 ... LOCATION Residence/Home

    The arrest emails (and older crime emails) put each field on its
    own line. We collapse both formats into the line-per-field form by
    finding every known label that appears mid-line and injecting a
    newline before it. Idempotent — labels that are already at the
    start of a line are left alone.

    Long labels are matched first ("Arrest Location PSA" before "PSA"),
    so the more specific label wins when one is a prefix of another.
    """
    labels = sorted({label for _, label in fields}, key=len, reverse=True)
    # ``(?<=\S)`` ensures we only break at labels that are NOT already
    # at line start (i.e. preceded by a non-whitespace char). The
    # following ``\s+`` consumes the preceding spaces — including
    # ``\xa0`` non-breaking spaces MPD's emails use — and is replaced
    # by a single newline. ``\s+`` after the label keeps the value's
    # whitespace intact.
    pattern = re.compile(
        r"(?<=\S)\s+(?=(?:" + "|".join(re.escape(l) for l in labels) + r")\s)",
    )
    return pattern.sub("\n", text)


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
    last_key: str | None = None
    started = False
    start_key = next(k for k, label in fields if label == start_field)

    # Pre-pass: collapse inline-label format into line-per-field form,
    # so the rest of the loop only deals with one shape.
    text = _normalize_body(text, fields)

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if _FOOTER_RE.match(line):
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
            last_key = None
        if current is None:
            current = {}

        key, value = _split_label(line, fields=fields)
        if key:
            current[key] = value
            # Only persist last_key for fields that legitimately wrap.
            # After a single-line field, last_key resets so any stray
            # non-label line that follows (e.g. the "Reminder:" prose
            # paragraph MPD embeds mid-email) gets ignored instead of
            # appended.
            last_key = key if key in _CONTINUABLE_FIELDS else None
        elif last_key:
            # Continuation line for a known multi-line field.
            current[last_key] = (current[last_key] + " " + line).strip()
        # Other non-label lines (boilerplate paragraphs between record
        # batches, image alt-text, etc) are silently ignored.

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
            arrest_location=clean_arrest_location(rec.get("arrest_location")),
            offender_first_name=rec.get("offender_first_name"),
            offender_last_name=rec.get("offender_last_name"),
            gender=rec.get("gender"),
            age=age,
            offense=rec.get("offense"),
            felony_misdemeanor=(rec.get("felony_misdemeanor") or None),
            officer=rec.get("officer"),
        ))
    return records
