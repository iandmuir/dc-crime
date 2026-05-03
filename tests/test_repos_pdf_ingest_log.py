"""Tests for repos.pdf_ingest_log — append + latest-per-(district,kind)."""
import time

from wswdy.repos.pdf_ingest_log import latest_per_district_kind, recent, record


def test_latest_picks_most_recent_row_per_district_kind(db):
    record(db, district="1D", kind="crime", source_file="old.pdf", records=3)
    # Force a different timestamp; SQLite's CURRENT_TIMESTAMP is per-second
    # so we sleep just enough for an ordered MAX(ingested_at) result.
    time.sleep(1.05)
    record(db, district="1D", kind="crime", source_file="new.pdf", records=4)
    record(db, district="2D", kind="crime", source_file="2d.pdf", records=2)
    record(db, district="1D", kind="arrest", source_file="1d-a.pdf", records=5)

    latest = latest_per_district_kind(db)
    by_key = {(r["district"], r["kind"]): r for r in latest}

    # 1D/crime should be the second (newer) ingest
    assert by_key[("1D", "crime")]["source_file"] == "new.pdf"
    assert by_key[("1D", "crime")]["records"] == 4
    # Other (district, kind) combinations come through unchanged
    assert by_key[("2D", "crime")]["source_file"] == "2d.pdf"
    assert by_key[("1D", "arrest")]["source_file"] == "1d-a.pdf"
    # Exactly one row per (district, kind)
    assert len(latest) == 3


def test_recent_returns_newest_first(db):
    record(db, district="1D", kind="crime", source_file="a.pdf", records=1)
    time.sleep(1.05)
    record(db, district="1D", kind="crime", source_file="b.pdf", records=1)
    rows = recent(db, limit=10)
    assert rows[0]["source_file"] == "b.pdf"
    assert rows[1]["source_file"] == "a.pdf"


def test_latest_empty_when_no_rows(db):
    assert latest_per_district_kind(db) == []
