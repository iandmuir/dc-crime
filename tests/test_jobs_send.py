from unittest.mock import AsyncMock, patch

from wswdy.alerts import AdminAlerter
from wswdy.jobs.send import (
    feed_has_yesterdays_data,
    run_daily_sends,
    run_send_if_ready,
)
from wswdy.notifiers.fake import FakeNotifier
from wswdy.repos.crimes import upsert_many
from wswdy.repos.send_log import exists_for_today
from wswdy.repos.subscribers import insert_pending, set_status


def _seed_subscriber(db, sid="s1", channel="email"):
    insert_pending(db, sid=sid, display_name="Jane",
                   email="jane@example.com" if channel == "email" else None,
                   phone="+12025551234" if channel == "whatsapp" else None,
                   preferred_channel=channel,
                   address_text="1500 14th St NW",
                   lat=38.9097, lon=-77.0319, radius_m=1000)
    set_status(db, sid, "APPROVED")


def _seed_crime(db, ccn="C1", offense="THEFT/OTHER",
                lat=38.9100, lon=-77.0319, when_iso=None):
    upsert_many(db, [{
        "ccn": ccn, "offense": offense, "method": None, "shift": "DAY",
        "block_address": "x", "lat": lat, "lon": lon,
        "report_dt": when_iso or "2026-04-27T12:00:00Z",
        "start_dt": None, "end_dt": None, "ward": "2", "district": "3",
        "raw_json": "{}",
    }])


async def test_send_daily_emails_active_subscriber(db, tmp_path):
    _seed_subscriber(db, "s1", channel="email")
    _seed_crime(db, when_iso="2026-04-27T15:00:00Z")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_daily_sends(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        send_date="2026-04-28", now_iso="2026-04-28T10:00:00+00:00",
        stagger=False, render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["sent"] == 1
    assert email.sent and "Jane" in email.sent[0]["text"]
    assert exists_for_today(db, "s1", "2026-04-28", "email")


async def test_send_skips_already_sent_today(db, tmp_path):
    _seed_subscriber(db, "s1", channel="email")
    _seed_crime(db)
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    args = dict(db=db, email=email, whatsapp=wa, alerter=alerter,
                base_url="https://x", hmac_secret="s",
                send_date="2026-04-28", now_iso="2026-04-28T10:00:00+00:00",
                stagger=False,
                render_static_map=AsyncMock(return_value=tmp_path / "p.png"))
    await run_daily_sends(**args)
    second = await run_daily_sends(**args)
    assert second["sent"] == 0
    assert second["skipped"] == 1
    assert len(email.sent) == 1


async def test_send_does_not_crash_on_naive_fetched_at(db, tmp_path):
    """Regression: SQLite's CURRENT_TIMESTAMP stores fetched_at as a tz-naive
    'YYYY-MM-DD HH:MM:SS' string. Comparing it to a tz-aware now_iso used to
    raise TypeError ("can't subtract offset-naive and offset-aware datetimes")
    and crash the entire send job before any subscriber was processed."""
    from wswdy.repos.fetch_log import record_success
    record_success(db, added=10, updated=5)  # uses CURRENT_TIMESTAMP -> naive
    # Sanity-check the regression precondition: the stored format really is
    # tz-naive. If schema changes ever store with timezone, this test still
    # passes — but the original bug couldn't have happened.
    fetched_at = db.execute(
        "SELECT fetched_at FROM fetch_log ORDER BY id DESC LIMIT 1"
    ).fetchone()[0]
    assert "+" not in fetched_at and "Z" not in fetched_at, (
        f"expected naive timestamp, got {fetched_at!r}"
    )

    _seed_subscriber(db, "s1", channel="email")
    _seed_crime(db)
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_daily_sends(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        send_date="2026-04-29", now_iso="2026-04-29T10:00:00+00:00",
        stagger=False, render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["sent"] == 1


async def test_send_appends_mpd_warning_when_feed_stale(db, tmp_path):
    """If most recent fetch failed and last successful is >24h old, append warning."""
    from wswdy.repos.fetch_log import record_failure, record_success
    record_success(db, added=0, updated=0)  # initial
    # Simulate: last successful was 2 days ago, then a failure today
    db.execute("UPDATE fetch_log SET fetched_at='2026-04-26T05:30:00+00:00'")
    record_failure(db, error="503")
    db.commit()

    _seed_subscriber(db, "s1", channel="email")
    _seed_crime(db)
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    await run_daily_sends(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        send_date="2026-04-28", now_iso="2026-04-28T10:00:00+00:00",
        stagger=False, render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert "MPD" in email.sent[0]["text"] or "delayed" in email.sent[0]["text"].lower()


async def test_send_logs_failure_and_continues(db, tmp_path):
    _seed_subscriber(db, "ok", channel="email")
    _seed_subscriber(db, "fail", channel="email")
    _seed_crime(db)

    email = FakeNotifier()  # ok for "ok"
    failing = FakeNotifier(fail_with="smtp 530")

    # Patch dispatch so the second subscriber sees the failing notifier
    async def patched_dispatch(sub, **kw):
        if sub["id"] == "fail":
            return await failing.send(recipient=sub["email"], subject=kw["subject"],
                                      text=kw["text"], image_path=kw["image_path"])
        return await email.send(recipient=sub["email"], subject=kw["subject"],
                                text=kw["text"], image_path=kw["image_path"])

    with patch("wswdy.jobs.send.dispatch", new=patched_dispatch):
        alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                               ha_webhook_url="", suppression_hours=6)
        out = await run_daily_sends(
            db=db, email=email, whatsapp=failing, alerter=alerter,
            base_url="https://x", hmac_secret="s",
            send_date="2026-04-28", now_iso="2026-04-28T10:00:00+00:00",
            stagger=False, render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
        )
    assert out["sent"] == 1
    assert out["failed"] == 1


# ----- Adaptive send (run_send_if_ready) ----------------------------------

def test_feed_has_yesterdays_data_false_when_only_older_records(db):
    """Feed with no records from yesterday's ET date returns False."""
    # Seed a record from 2 days ago
    _seed_crime(db, ccn="X1", when_iso="2026-04-27T15:00:00Z")
    # "now" is 2026-04-29 morning ET (UTC offset 4h, so 13:00 UTC ~= 9:00 EDT)
    assert not feed_has_yesterdays_data(db, now_iso="2026-04-29T13:00:00+00:00")


def test_feed_has_yesterdays_data_true_when_batch_landed(db):
    """Once the MPD batch arrives with several reports from yesterday, freshness flips."""
    # Yesterday in ET = 2026-04-28. Seed 6 records from that day (above the
    # min_records=5 threshold).
    for i in range(6):
        _seed_crime(db, ccn=f"Y{i}", when_iso=f"2026-04-28T{10+i:02d}:00:00Z")
    assert feed_has_yesterdays_data(db, now_iso="2026-04-29T13:00:00+00:00")


def test_feed_has_yesterdays_data_ignores_single_straggler(db):
    """A single early-published record from yesterday shouldn't count as 'batch in'."""
    _seed_crime(db, ccn="S1", when_iso="2026-04-28T22:00:00Z")
    assert not feed_has_yesterdays_data(db, now_iso="2026-04-29T13:00:00+00:00")


async def test_run_send_if_ready_waits_when_feed_stale_before_cutoff(db, tmp_path):
    """Adaptive send: stale feed + before cutoff -> waiting, no dispatch fires."""
    _seed_subscriber(db, "s1", channel="email")
    _seed_crime(db, ccn="OLD", when_iso="2026-04-27T15:00:00Z")  # 2 days ago
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_send_if_ready(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        now_iso="2026-04-29T13:00:00+00:00",  # 9 AM ET, before 7 PM cutoff
        cutoff_hour_et=19,
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["status"] == "waiting_for_fresh_data"
    assert email.sent == []


async def test_run_send_if_ready_fires_when_feed_fresh(db, tmp_path):
    """Adaptive send: feed has yesterday's data -> dispatch fires immediately."""
    _seed_subscriber(db, "s1", channel="email")
    for i in range(6):
        _seed_crime(db, ccn=f"Y{i}", when_iso=f"2026-04-28T{10+i:02d}:00:00Z")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_send_if_ready(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        now_iso="2026-04-29T13:00:00+00:00",  # 9 AM ET
        cutoff_hour_et=19,
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["status"] == "sent"
    assert out["sent"] == 1


async def test_run_send_if_ready_force_sends_at_cutoff(db, tmp_path):
    """Adaptive send: stale feed + past cutoff -> force-send anyway."""
    _seed_subscriber(db, "s1", channel="email")
    _seed_crime(db, ccn="OLD", when_iso="2026-04-27T15:00:00Z")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_send_if_ready(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        now_iso="2026-04-29T23:30:00+00:00",  # 7:30 PM ET, past cutoff
        cutoff_hour_et=19,
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["status"] == "sent_at_cutoff"
    assert out["sent"] == 1


async def test_run_send_if_ready_skips_if_already_sent(db, tmp_path):
    """Adaptive send: a send already recorded for today -> noop on subsequent triggers."""
    _seed_subscriber(db, "s1", channel="email")
    for i in range(6):
        _seed_crime(db, ccn=f"Y{i}", when_iso=f"2026-04-28T{10+i:02d}:00:00Z")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    common = dict(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        now_iso="2026-04-29T13:00:00+00:00",
        cutoff_hour_et=19,
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    first = await run_send_if_ready(**common)
    assert first["status"] == "sent"
    second = await run_send_if_ready(**common)
    assert second["status"] == "already_sent_today"
    # Only one message went out across both calls.
    assert len(email.sent) == 1
