"""Ingest one MPD daily LISTSERV email into the database.

Bridges :mod:`wswdy.email_reports` (the parser) and the :mod:`wswdy.repos`
layer. Sibling to :mod:`wswdy.jobs.pdf_ingest` — same destination tables,
different source format. Where pdf_ingest reads bytes off disk, this
module reads the JSON returned by Resend's "Received emails" API.

The email body is the **canonical** source for production: MPD's PDF
attachments are inconsistent (the attached PDF is sometimes for a
different district than the email is about), but the email subject and
body always match. The PDF path is kept for the manual CLI tool only.

Coverage tracking still runs through ``pdf_ingest_log`` (the table name is
historical — it tracks one row per (district, kind) report ingest, no
matter the source format).
"""
from __future__ import annotations

import logging

from wswdy.clients.maptiler import GeocodeError, geocode_address
from wswdy.email_reports import (
    detect_kind_from_subject,
    extract_district,
    parse_arrest_email,
    parse_crime_email,
)
from wswdy.repos import arrests as arrests_repo
from wswdy.repos import crime_extras as crime_extras_repo
from wswdy.repos import pdf_ingest_log as ingest_log_repo

log = logging.getLogger(__name__)


def _crime_record_to_extras(rec) -> dict:
    return {
        "ccn": rec.ccn,
        "district": rec.district,
        "psa": rec.psa,
        "location": rec.location,
        "source": "email_listserv",
    }


def _arrest_record_to_row(rec) -> dict:
    return {
        "arrest_number": rec.arrest_number,
        "district": rec.district,
        "psa": rec.psa,
        "arrest_dt": rec.arrest_dt,
        "arrest_location": rec.arrest_location,
        "lat": None,
        "lon": None,
        "offender_first": rec.offender_first_name,
        "offender_last": rec.offender_last_name,
        "gender": rec.gender,
        "age": rec.age,
        "offense": rec.offense,
        "felony_misd": rec.felony_misdemeanor,
        "officer": rec.officer,
    }


async def _geocode_arrest_locations(
    db, *, api_key: str, limit: int,
) -> tuple[int, int]:
    """Backfill lat/lon for arrests that don't have them yet.

    Same shape and rationale as ``jobs.pdf_ingest._geocode_arrest_locations``;
    duplicated here (rather than imported) so each ingest path stays
    self-contained.
    """
    if not api_key:
        log.debug("no MapTiler key configured — skipping arrest geocoding")
        return 0, 0
    pending = arrests_repo.list_missing_geocode(db, limit=limit)
    ok = failed = 0
    for row in pending:
        addr = (row.get("arrest_location") or "").strip()
        if not addr:
            failed += 1
            continue
        try:
            res = await geocode_address(addr, api_key=api_key)
            arrests_repo.update_geocode(
                db, row["arrest_number"], res["lat"], res["lon"],
            )
            ok += 1
        except (GeocodeError, Exception) as e:  # noqa: BLE001
            log.info("geocode failed for arrest %s (%r): %s",
                     row["arrest_number"], addr, e)
            failed += 1
    return ok, failed


async def ingest_email(
    *, db, subject: str, text_body: str, source_file: str | None = None,
    maptiler_api_key: str = "",
) -> dict:
    """Parse + persist a single MPD email. Auto-detects kind from subject.

    ``source_file`` (optional) is recorded in pdf_ingest_log for the
    admin coverage tracker — pass the email_id or message_id for a
    useful audit trail.
    """
    kind = detect_kind_from_subject(subject)
    if kind == "crime":
        return await _ingest_crime_email(
            db=db, subject=subject, text_body=text_body,
            source_file=source_file,
        )
    if kind == "arrest":
        return await _ingest_arrest_email(
            db=db, subject=subject, text_body=text_body,
            source_file=source_file, maptiler_api_key=maptiler_api_key,
        )
    log.warning("could not detect kind from subject %r — skipping", subject)
    return {"status": "skipped", "reason": "unknown kind", "subject": subject}


async def _ingest_crime_email(
    *, db, subject: str, text_body: str, source_file: str | None,
) -> dict:
    records = parse_crime_email(subject=subject, text_body=text_body)
    district = extract_district(subject, text_body) or "unknown"
    if not records:
        log.warning("crime email (%s) yielded 0 records", district)
        return {"status": "empty", "kind": "crime", "district": district,
                "added": 0, "updated": 0}

    rows = [_crime_record_to_extras(r) for r in records]
    added, updated = crime_extras_repo.upsert_many(db, rows)

    ingest_log_repo.record(
        db, district=district, kind="crime",
        source_file=source_file, records=len(records),
    )
    log.info("crime email ingest (%s): +%d / ~%d", district, added, updated)
    return {
        "status": "ok",
        "kind": "crime",
        "district": district,
        "added": added,
        "updated": updated,
        "total": len(records),
    }


async def _ingest_arrest_email(
    *, db, subject: str, text_body: str, source_file: str | None,
    maptiler_api_key: str,
) -> dict:
    records = parse_arrest_email(subject=subject, text_body=text_body)
    district = extract_district(subject, text_body) or "unknown"
    if not records:
        log.warning("arrest email (%s) yielded 0 records", district)
        return {"status": "empty", "kind": "arrest", "district": district,
                "added": 0, "updated": 0}

    rows = [_arrest_record_to_row(r) for r in records]
    added, updated = arrests_repo.upsert_many(db, rows)

    geo_ok, geo_failed = await _geocode_arrest_locations(
        db, api_key=maptiler_api_key, limit=len(records) + 50,
    )

    ingest_log_repo.record(
        db, district=district, kind="arrest",
        source_file=source_file, records=len(records),
    )
    log.info("arrest email ingest (%s): +%d / ~%d, geocoded %d/%d",
             district, added, updated, geo_ok, geo_ok + geo_failed)
    return {
        "status": "ok",
        "kind": "arrest",
        "district": district,
        "added": added,
        "updated": updated,
        "total": len(records),
        "geocoded": geo_ok,
        "geocode_failed": geo_failed,
    }
