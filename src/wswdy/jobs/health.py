"""Daily 23:00 ET health snapshot email to the admin."""
import sqlite3

from wswdy.notifiers.base import Notifier
from wswdy.repos.fetch_log import last_attempt
from wswdy.repos.send_log import recent_failures, send_volume_last_n_days
from wswdy.repos.subscribers import list_by_status


async def run_health_snapshot(
    *, db: sqlite3.Connection, email: Notifier, admin_email: str, today: str
) -> dict:
    """Send a daily health summary email to the admin."""
    pending = len(list_by_status(db, "PENDING"))
    approved = len(list_by_status(db, "APPROVED"))
    unsub = len(list_by_status(db, "UNSUBSCRIBED"))
    last_fetch = last_attempt(db) or {}
    today_volume = [
        r for r in send_volume_last_n_days(db, n=1, today=today)
        if r["send_date"] == today
    ]
    sent_count = today_volume[0]["sent"] if today_volume else 0
    failed_count = today_volume[0]["failed"] if today_volume else 0
    fails = recent_failures(db, limit=5)

    lines = [
        f"WTFDC daily health — {today}",
        "",
        f"Subscribers: {approved} approved · {pending} pending · {unsub} unsubscribed",
        (
            f"MPD fetch:   {last_fetch.get('status', 'never')} "
            f"(+{last_fetch.get('crimes_added') or 0}, "
            f"~{last_fetch.get('crimes_updated') or 0}) "
            f"at {last_fetch.get('fetched_at', 'n/a')}"
        ),
        f"Sends today: {sent_count} sent · {failed_count} failed",
    ]
    if fails:
        lines.append("")
        lines.append("Recent failures:")
        for f in fails:
            lines.append(f"  · {f['subscriber_id']} ({f['channel']}): {f['error']}")

    text = "\n".join(lines)
    res = await email.send(
        recipient=admin_email,
        subject=f"[WTFDC] daily snapshot {today}",
        text=text,
        image_path=None,
    )
    return {"sent": 1 if res.ok else 0, "error": res.error}
