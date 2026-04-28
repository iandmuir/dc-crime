from wswdy.repos.send_log import record, exists_for_today, recent_failures, send_volume_last_n_days
from wswdy.repos.subscribers import insert_pending


def _sub(db, sid="s1"):
    insert_pending(db, sid=sid, display_name="J", email="j@x.com", phone=None,
                   preferred_channel="email", address_text="x",
                   lat=38.9, lon=-77.0, radius_m=1000)
    return sid


def test_record_and_exists(db):
    _sub(db)
    record(db, "s1", "2026-04-28", "email", "sent")
    assert exists_for_today(db, "s1", "2026-04-28", "email") is True
    assert exists_for_today(db, "s1", "2026-04-28", "whatsapp") is False


def test_record_idempotent_unique_constraint(db):
    _sub(db)
    record(db, "s1", "2026-04-28", "email", "sent")
    # Re-recording the same (sid, date, channel) should be a no-op, not an error.
    record(db, "s1", "2026-04-28", "email", "sent")
    rows = db.execute("SELECT COUNT(*) FROM send_log").fetchone()[0]
    assert rows == 1


def test_recent_failures(db):
    _sub(db, "s1"); _sub(db, "s2")
    record(db, "s1", "2026-04-28", "email", "failed", error="smtp 530")
    record(db, "s2", "2026-04-28", "email", "sent")
    fails = recent_failures(db, limit=10)
    assert len(fails) == 1
    assert fails[0]["subscriber_id"] == "s1"
    assert fails[0]["error"] == "smtp 530"


def test_send_volume_last_n_days(db):
    _sub(db)
    record(db, "s1", "2026-04-26", "email", "sent")
    record(db, "s1", "2026-04-27", "email", "sent")
    record(db, "s1", "2026-04-28", "email", "failed")
    rows = send_volume_last_n_days(db, n=7, today="2026-04-28")
    # rows is a list of dicts with date, sent, failed counts
    by_date = {r["send_date"]: r for r in rows}
    assert by_date["2026-04-26"]["sent"] == 1
    assert by_date["2026-04-28"]["failed"] == 1
