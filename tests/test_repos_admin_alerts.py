from datetime import datetime, timedelta, timezone
from wswdy.repos.admin_alerts import (
    record, is_suppressed, set_suppressed_until, list_recent,
)


def _now():
    return datetime.now(timezone.utc)


def test_record_creates_row(db):
    record(db, alert_type="mpd_down", message="MPD 503")
    rows = db.execute("SELECT * FROM admin_alerts").fetchall()
    assert len(rows) == 1
    assert rows[0]["alert_type"] == "mpd_down"


def test_is_suppressed_false_when_no_recent(db):
    assert is_suppressed(db, "mpd_down") is False


def test_set_and_check_suppression(db):
    until = (_now() + timedelta(hours=1)).isoformat()
    set_suppressed_until(db, "mpd_down", until)
    assert is_suppressed(db, "mpd_down") is True


def test_suppression_expires(db):
    past = (_now() - timedelta(hours=1)).isoformat()
    set_suppressed_until(db, "mpd_down", past)
    assert is_suppressed(db, "mpd_down") is False


def test_list_recent(db):
    record(db, alert_type="x", message="m1")
    record(db, alert_type="y", message="m2")
    rows = list_recent(db, limit=10)
    assert len(rows) == 2


def test_list_recent_excludes_suppression_markers(db):
    record(db, alert_type="x", message="m1")
    set_suppressed_until(db, "x", (_now() + timedelta(hours=1)).isoformat())
    rows = list_recent(db, limit=10)
    assert len(rows) == 1  # suppression marker excluded
