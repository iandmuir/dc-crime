"""Tests for repos.arrests — upsert, geocode backfill, radius+window queries."""
from datetime import UTC, datetime, timedelta

from wswdy.repos.arrests import (
    list_in_radius_window,
    list_missing_geocode,
    prune_older_than,
    update_geocode,
    upsert_many,
)


def _arrest(arrest_number: str, *, lat: float | None = 38.9097,
            lon: float | None = -77.0319, when: str | None = None,
            felony: str | None = "MISDEMEANOR") -> dict:
    return {
        "arrest_number": arrest_number,
        "district": "2D",
        "psa": "202",
        "arrest_dt": when or "2026-04-27T16:00:00Z",
        "arrest_location": "5028 Belt Rd NW Washington DC",
        "lat": lat, "lon": lon,
        "offender_first": "Jane",
        "offender_last": "Doe",
        "gender": "Female",
        "age": 30,
        "offense": "Simple Assault",
        "felony_misd": felony,
        "officer": "Smith 12345",
    }


def test_upsert_inserts_new(db):
    added, updated = upsert_many(db, [_arrest("A1"), _arrest("A2")])
    assert (added, updated) == (2, 0)


def test_upsert_updates_on_same_arrest_number(db):
    upsert_many(db, [_arrest("A1", felony="MISDEMEANOR")])
    added, updated = upsert_many(db, [_arrest("A1", felony="FELONY")])
    assert (added, updated) == (0, 1)
    row = db.execute(
        "SELECT felony_misd FROM arrests WHERE arrest_number='A1'"
    ).fetchone()
    assert row["felony_misd"] == "FELONY"


def test_list_missing_geocode(db):
    upsert_many(db, [
        _arrest("geo-ok", lat=38.9, lon=-77.0),
        _arrest("geo-missing", lat=None, lon=None),
    ])
    rows = list_missing_geocode(db)
    assert {r["arrest_number"] for r in rows} == {"geo-missing"}


def test_update_geocode_backfills(db):
    upsert_many(db, [_arrest("A", lat=None, lon=None)])
    update_geocode(db, "A", 38.9097, -77.0319)
    row = db.execute("SELECT lat, lon FROM arrests WHERE arrest_number='A'").fetchone()
    assert row["lat"] == 38.9097 and row["lon"] == -77.0319


def test_list_in_radius_window_excludes_ungeocoded(db):
    now = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
    upsert_many(db, [
        _arrest("recent", lat=38.9097, lon=-77.0319,
                when=(now - timedelta(hours=2)).isoformat()),
        _arrest("ungeocoded", lat=None, lon=None,
                when=(now - timedelta(hours=2)).isoformat()),
        _arrest("too-old", lat=38.9097, lon=-77.0319,
                when=(now - timedelta(days=10)).isoformat()),
        _arrest("too-far", lat=38.95, lon=-77.05,
                when=(now - timedelta(hours=2)).isoformat()),
    ])
    rows = list_in_radius_window(
        db, 38.9097, -77.0319, 500,
        start=(now - timedelta(hours=24)).isoformat(),
        end=now.isoformat(),
    )
    assert {r["arrest_number"] for r in rows} == {"recent"}


def test_prune_older_than(db):
    upsert_many(db, [
        _arrest("old", when="2025-01-01T00:00:00Z"),
        _arrest("new", when="2026-04-27T00:00:00Z"),
    ])
    deleted = prune_older_than(db, "2026-01-01T00:00:00Z")
    assert deleted == 1
