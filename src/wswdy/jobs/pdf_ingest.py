"""Ingest one or more MPD daily LISTSERV PDFs into the database.

Bridges wswdy.pdf_reports (the parser) and the repos/* layer:

  - **Crime PDFs** → upserts into `crime_extras` keyed by CCN. The canonical
    crime row still comes from the public ArcGIS feed; we just decorate it
    with the LISTSERV-only `location` enum.

  - **Arrest PDFs** → upserts into `arrests`. Each new arrest is geocoded
    once (MapTiler) at ingest time; failed geocodes leave lat/lon NULL and
    the row is excluded from spatial queries until backfilled.

In both cases we record a `pdf_ingest_log` row keyed by (district, kind) so
the admin coverage tracker knows which districts have shipped today's PDF.

The job is intentionally idempotent — re-running over the same PDF refreshes
existing rows without duplicating, and does NOT re-geocode rows that already
have a lat/lon. This makes manual replays safe.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import asdict
from pathlib import Path

from wswdy.clients.maptiler import GeocodeError, geocode_address
from wswdy.pdf_reports import (
    detect_pdf_kind,
    parse_arrest_pdf,
    parse_crime_pdf,
)
from wswdy.repos import arrests as arrests_repo
from wswdy.repos import crime_extras as crime_extras_repo
from wswdy.repos import pdf_ingest_log as ingest_log_repo

log = logging.getLogger(__name__)


def _crime_record_to_extras(rec) -> dict:
    """Map CrimePdfRecord → crime_extras row dict."""
    return {
        "ccn": rec.ccn,
        "district": rec.district,
        "psa": rec.psa,
        "location": rec.location,
        "source": "pdf_listserv",
    }


def _arrest_record_to_row(rec) -> dict:
    """Map ArrestPdfRecord → arrests row dict (sans lat/lon — geocoded later)."""
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


def _district_of(records) -> str | None:
    """All records from one PDF share a district — pick the first non-null."""
    for r in records:
        if r.district:
            return r.district
    return None


async def ingest_crime_pdf(
    *, db: sqlite3.Connection, path: Path | str,
) -> dict:
    """Parse + persist a single crime PDF. Logs coverage on success."""
    path = Path(path)
    records = parse_crime_pdf(path)
    if not records:
        log.warning("crime PDF %s yielded 0 records", path.name)
        return {"status": "empty", "added": 0, "updated": 0}

    rows = [_crime_record_to_extras(r) for r in records]
    added, updated = crime_extras_repo.upsert_many(db, rows)
    district = _district_of(records) or "unknown"

    ingest_log_repo.record(
        db, district=district, kind="crime",
        source_file=path.name, records=len(records),
    )
    log.info("crime PDF ingest %s (%s): +%d / ~%d",
             path.name, district, added, updated)
    return {
        "status": "ok",
        "kind": "crime",
        "district": district,
        "added": added,
        "updated": updated,
        "total": len(records),
    }


async def _geocode_arrest_locations(
    db: sqlite3.Connection, *, api_key: str, limit: int | None = None,
) -> tuple[int, int]:
    """Geocode arrests that don't have a lat/lon yet. Returns (ok, failed).

    Pulled out as a separate step (not done inline during upsert) so the
    upsert stays a fast sync operation and a flaky MapTiler doesn't block
    PDF ingestion.
    """
    if not api_key:
        log.debug("no MapTiler key configured — skipping arrest geocoding")
        return 0, 0
    pending = arrests_repo.list_missing_geocode(db, limit=limit or 100)
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
        except (GeocodeError, Exception) as e:  # noqa: BLE001 - log + continue
            log.info("geocode failed for arrest %s (%r): %s",
                     row["arrest_number"], addr, e)
            failed += 1
    return ok, failed


async def ingest_arrest_pdf(
    *, db: sqlite3.Connection, path: Path | str, maptiler_api_key: str = "",
) -> dict:
    """Parse + persist a single arrest PDF, then geocode any new rows."""
    path = Path(path)
    records = parse_arrest_pdf(path)
    if not records:
        log.warning("arrest PDF %s yielded 0 records", path.name)
        return {"status": "empty", "added": 0, "updated": 0}

    rows = [_arrest_record_to_row(r) for r in records]
    added, updated = arrests_repo.upsert_many(db, rows)
    district = _district_of(records) or "unknown"

    geo_ok, geo_failed = await _geocode_arrest_locations(
        db, api_key=maptiler_api_key, limit=len(records) + 50,
    )

    ingest_log_repo.record(
        db, district=district, kind="arrest",
        source_file=path.name, records=len(records),
    )
    log.info("arrest PDF ingest %s (%s): +%d / ~%d, geocoded %d/%d",
             path.name, district, added, updated, geo_ok, geo_ok + geo_failed)
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


async def ingest_pdf(
    *, db: sqlite3.Connection, path: Path | str, maptiler_api_key: str = "",
) -> dict:
    """Auto-route a single PDF to the right ingester based on header text."""
    path = Path(path)
    kind = detect_pdf_kind(path)
    if kind == "crime":
        return await ingest_crime_pdf(db=db, path=path)
    if kind == "arrest":
        return await ingest_arrest_pdf(
            db=db, path=path, maptiler_api_key=maptiler_api_key,
        )
    log.warning("could not detect kind of %s — skipping", path.name)
    return {"status": "skipped", "reason": "unknown kind"}


# Conveniences for tests / scripts -----------------------------------------

def crime_record_dicts(records) -> list[dict]:
    """Helper for the CLI: dump CrimePdfRecord list as plain dicts."""
    return [asdict(r) for r in records]
