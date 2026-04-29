from datetime import UTC, datetime, timedelta

from wswdy.repos.crimes import (
    count_in_radius,
    list_in_radius,
    list_in_radius_window,
    prune_older_than,
    upsert_many,
)


def _crime(ccn, offense="THEFT/OTHER", method=None, lat=38.9097, lon=-77.0319,
           when=None, raw=None):
    return {
        "ccn": ccn, "offense": offense, "method": method, "shift": "DAY",
        "block_address": "1400 block of P St NW",
        "lat": lat, "lon": lon,
        "report_dt": when or "2026-04-27T12:00:00Z",
        "start_dt": None, "end_dt": None,
        "ward": "2", "district": "THIRD",
        "raw_json": raw or "{}",
    }


def test_upsert_inserts_new(db):
    n_added, n_updated = upsert_many(db, [_crime("C1"), _crime("C2")])
    assert (n_added, n_updated) == (2, 0)


def test_upsert_updates_existing_on_same_ccn(db):
    upsert_many(db, [_crime("C1", offense="THEFT/OTHER")])
    n_added, n_updated = upsert_many(db, [_crime("C1", offense="ARSON")])
    assert (n_added, n_updated) == (0, 1)
    rows = db.execute("SELECT offense FROM crimes WHERE ccn='C1'").fetchall()
    assert rows[0]["offense"] == "ARSON"


def test_count_in_radius(db):
    upsert_many(db, [
        _crime("near1", lat=38.9097, lon=-77.0319),                       # 0 m
        _crime("near2", lat=38.9100, lon=-77.0319),                       # ~33 m
        _crime("far",   lat=38.9300, lon=-77.0500),                       # ~3 km
    ])
    n = count_in_radius(db, 38.9097, -77.0319, 500)
    assert n == 2


def test_list_in_radius_filters_correctly(db):
    upsert_many(db, [
        _crime("a", lat=38.9097, lon=-77.0319, when="2026-04-27T08:00:00Z"),
        _crime("b", lat=38.9099, lon=-77.0319, when="2026-04-27T09:00:00Z"),
        _crime("c", lat=38.9500, lon=-77.0500, when="2026-04-27T10:00:00Z"),
    ])
    rows = list_in_radius(db, 38.9097, -77.0319, 500)
    ccns = {r["ccn"] for r in rows}
    assert ccns == {"a", "b"}


def test_list_in_radius_window_24h(db):
    now = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
    upsert_many(db, [
        _crime("recent", when=(now - timedelta(hours=2)).isoformat()),
        _crime("oldish", when=(now - timedelta(hours=30)).isoformat()),
        _crime("ancient", when=(now - timedelta(days=10)).isoformat()),
    ])
    rows = list_in_radius_window(db, 38.9097, -77.0319, 500,
                                 start=(now - timedelta(hours=24)).isoformat(),
                                 end=now.isoformat())
    assert {r["ccn"] for r in rows} == {"recent"}


def test_prune_deletes_old(db):
    old = "2025-01-01T00:00:00Z"
    new = "2026-04-27T00:00:00Z"
    upsert_many(db, [_crime("old", when=old), _crime("new", when=new)])
    deleted = prune_older_than(db, "2026-01-01T00:00:00Z")
    assert deleted == 1
    remaining = db.execute("SELECT ccn FROM crimes").fetchall()
    assert [r["ccn"] for r in remaining] == ["new"]
