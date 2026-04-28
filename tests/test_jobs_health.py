from wswdy.jobs.health import run_health_snapshot
from wswdy.notifiers.fake import FakeNotifier
from wswdy.repos.fetch_log import record_success
from wswdy.repos.send_log import record
from wswdy.repos.subscribers import insert_pending, set_status


async def test_health_snapshot_emails_admin(db):
    insert_pending(db, sid="a", display_name="A", email="a@x", phone=None,
                   preferred_channel="email", address_text="x",
                   lat=38.9, lon=-77.0, radius_m=1000)
    set_status(db, "a", "APPROVED")
    record_success(db, added=42, updated=3)
    record(db, "a", "2026-04-28", "email", "sent")

    email = FakeNotifier()
    out = await run_health_snapshot(db=db, email=email, admin_email="admin@x",
                                    today="2026-04-28")
    assert out["sent"] == 1
    assert email.sent[0]["recipient"] == "admin@x"
    body = email.sent[0]["text"]
    assert "fetched 42" in body or "+42" in body
    assert "1 sent" in body or "sent: 1" in body.lower()
