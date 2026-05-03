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
        send_date="2026-04-28", now_iso="2026-04-28T13:00:00+00:00",
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
                send_date="2026-04-28", now_iso="2026-04-28T13:00:00+00:00",
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
        send_date="2026-04-28", now_iso="2026-04-28T13:00:00+00:00",
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
            send_date="2026-04-28", now_iso="2026-04-28T13:00:00+00:00",
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


def _seed_listserv_today(
    db, *, now_iso: str, kinds=("crime", "arrest"), district="2D",
):
    """Helper: seed today's pdf_ingest_log so the readiness check passes.

    Writes ``ingested_at`` in SQLite's CURRENT_TIMESTAMP format (space
    separator, no offset) so the production WHERE-clause cutoff string
    sorts correctly against it. The test's mocked ``now_iso`` is passed
    in so the seeded timestamp lives on the same calendar day.
    """
    # Match the format wswdy.jobs.send.listserv_reports_in_today builds
    # for its cutoff comparison.
    from datetime import datetime as _dt
    naive = _dt.fromisoformat(now_iso.replace("Z", "+00:00"))
    ts = naive.strftime("%Y-%m-%d %H:%M:%S")
    for kind in kinds:
        db.execute(
            "INSERT INTO pdf_ingest_log "
            "(district, kind, source_file, records, ingested_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (district, kind, "test", 1, ts),
        )
    db.commit()


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
        now_iso="2026-04-29T13:00:00+00:00",  # 9 AM ET, before EoD cutoff
        cutoff_hour_et=19, cutoff_minute_et=0,
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["status"] == "waiting_for_fresh_feed"
    assert email.sent == []


async def test_run_send_if_ready_waits_when_listserv_emails_not_in(db, tmp_path):
    """Even when the API feed has yesterday's data, we still wait until
    today's crime + arrest LISTSERV emails arrive — at the per-subscriber
    level. With no district set on the subscriber, the readiness check
    falls back to global "any" — still False here because pdf_ingest_log
    is empty for today."""
    _seed_subscriber(db, "s1", channel="email")
    for i in range(6):
        _seed_crime(db, ccn=f"Y{i}", when_iso=f"2026-04-28T{10+i:02d}:00:00Z")
    # NOTE: no pdf_ingest_log rows seeded — emails haven't arrived yet.
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_send_if_ready(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        now_iso="2026-04-29T11:30:00+00:00",  # 7:30 AM ET, before 8:10 cutoff
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["status"] == "waiting_for_districts"
    assert out["sent"] == 0
    assert out["waiting"] == 1
    assert email.sent == []


async def test_run_send_if_ready_fires_when_everything_in(db, tmp_path):
    """Feed fresh + both LISTSERV emails today -> dispatch fires."""
    _seed_subscriber(db, "s1", channel="email")
    for i in range(6):
        _seed_crime(db, ccn=f"Y{i}", when_iso=f"2026-04-28T{10+i:02d}:00:00Z")
    _seed_listserv_today(db, now_iso="2026-04-29T13:00:00+00:00")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_send_if_ready(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        now_iso="2026-04-29T12:10:00+00:00",  # 8:10 AM ET — just at cutoff
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["status"] == "sent"
    assert out["sent"] == 1


async def test_run_send_if_ready_force_sends_at_cutoff(db, tmp_path):
    """Stale feed + past cutoff -> force-send anyway."""
    _seed_subscriber(db, "s1", channel="email")
    _seed_crime(db, ccn="OLD", when_iso="2026-04-27T15:00:00Z")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_send_if_ready(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        now_iso="2026-04-29T23:30:00+00:00",  # 7:30 PM ET — well past 8:10
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["status"] == "sent_at_cutoff"
    assert out["sent"] == 1


async def test_run_send_if_ready_skips_if_already_sent(db, tmp_path):
    """A send already recorded for today -> noop on subsequent triggers."""
    _seed_subscriber(db, "s1", channel="email")
    for i in range(6):
        _seed_crime(db, ccn=f"Y{i}", when_iso=f"2026-04-28T{10+i:02d}:00:00Z")
    _seed_listserv_today(db, now_iso="2026-04-29T13:00:00+00:00")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    common = dict(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        now_iso="2026-04-29T13:00:00+00:00",
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    first = await run_send_if_ready(**common)
    assert first["status"] == "sent"
    assert first["sent"] == 1
    second = await run_send_if_ready(**common)
    # Per-subscriber dedup: the second pass finds the send_log row from
    # the first call and skips that subscriber. Only one message total.
    assert second["sent"] == 0
    assert second["skipped"] == 1
    assert len(email.sent) == 1


# ---- Per-subscriber district readiness ----

async def test_run_daily_sends_waits_for_subscriber_district(db, tmp_path):
    """Subscriber tagged to 4D won't ship until 4D's reports land,
    even if 2D's are already in."""
    insert_pending(db, sid="s1", display_name="Jane",
                   email="jane@x", phone=None, preferred_channel="email",
                   address_text="x", lat=38.9097, lon=-77.0319,
                   radius_m=1000, district="4D")
    set_status(db, "s1", "APPROVED")
    # 2D is in but 4D is not.
    _seed_listserv_today(db, now_iso="2026-04-29T11:00:00+00:00", district="2D")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_daily_sends(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        send_date="2026-04-29",
        now_iso="2026-04-29T11:00:00+00:00",  # 7:00 AM ET, before cutoff
        stagger=False,
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["sent"] == 0
    assert out["waiting"] == 1


async def test_run_daily_sends_ships_when_districts_ready(db, tmp_path):
    """Once 4D's reports arrive, the previously-waiting subscriber ships."""
    insert_pending(db, sid="s1", display_name="Jane",
                   email="jane@x", phone=None, preferred_channel="email",
                   address_text="x", lat=38.9097, lon=-77.0319,
                   radius_m=1000, district="4D")
    set_status(db, "s1", "APPROVED")
    _seed_listserv_today(db, now_iso="2026-04-29T11:00:00+00:00", district="4D")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_daily_sends(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        send_date="2026-04-29",
        now_iso="2026-04-29T11:00:00+00:00",
        stagger=False,
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["sent"] == 1


async def test_run_daily_sends_force_ships_at_cutoff_even_if_districts_not_ready(
    db, tmp_path,
):
    """Past 8:10 ET we force-ship subscribers whose districts haven't
    completed — better an incomplete digest than none at all."""
    insert_pending(db, sid="s1", display_name="Jane",
                   email="jane@x", phone=None, preferred_channel="email",
                   address_text="x", lat=38.9097, lon=-77.0319,
                   radius_m=1000, district="4D")
    set_status(db, "s1", "APPROVED")
    # No reports for 4D today — only 2D.
    _seed_listserv_today(db, now_iso="2026-04-29T12:10:00+00:00", district="2D")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_daily_sends(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        send_date="2026-04-29",
        now_iso="2026-04-29T12:15:00+00:00",  # 8:15 AM ET — past cutoff
        stagger=False,
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["sent"] == 1


async def test_run_daily_sends_extra_districts_widen_the_gate(db, tmp_path):
    """A subscriber with extra_districts must wait for ALL of them."""
    insert_pending(db, sid="s1", display_name="Border",
                   email="b@x", phone=None, preferred_channel="email",
                   address_text="x", lat=38.9097, lon=-77.0319,
                   radius_m=1500, district="2D")
    set_status(db, "s1", "APPROVED")
    db.execute(
        "UPDATE subscribers SET extra_districts=? WHERE id=?", ("3D", "s1"),
    )
    db.commit()
    # 2D is in but 3D isn't — subscriber should wait.
    _seed_listserv_today(db, now_iso="2026-04-29T11:00:00+00:00", district="2D")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_daily_sends(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        send_date="2026-04-29",
        now_iso="2026-04-29T11:00:00+00:00",
        stagger=False,
        render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["sent"] == 0
    assert out["waiting"] == 1
