from wswdy.jobs.prune import run_prune
from wswdy.repos.crimes import upsert_many


def test_run_prune_deletes_crimes_older_than_90_days(db):
    upsert_many(db, [
        {"ccn": "old", "offense": "THEFT/OTHER", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9, "lon": -77.0,
         "report_dt": "2025-01-01T00:00:00Z",
         "start_dt": None, "end_dt": None, "ward": None, "district": None,
         "raw_json": "{}"},
        {"ccn": "new", "offense": "THEFT/OTHER", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9, "lon": -77.0,
         "report_dt": "2026-04-27T00:00:00Z",
         "start_dt": None, "end_dt": None, "ward": None, "district": None,
         "raw_json": "{}"},
    ])
    deleted = run_prune(db, today_iso="2026-04-28T00:00:00+00:00", days=90)
    assert deleted == 1
    rows = db.execute("SELECT ccn FROM crimes").fetchall()
    assert [r["ccn"] for r in rows] == ["new"]
