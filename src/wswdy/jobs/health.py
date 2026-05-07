"""Daily morning health snapshot email to the admin.

Runs after the morning send window closes (typically 9:00 ET) so the
snapshot can confirm today's digests went out — that's the whole point
of the email: spot a regression fast, before the day gets away from us."""
import sqlite3
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from wswdy.notifiers.base import Notifier
from wswdy.repos.fetch_log import last_attempt
from wswdy.repos.send_log import recent_failures, send_volume_last_n_days
from wswdy.repos.subscribers import list_by_status

ET = ZoneInfo("America/New_York")


def _fmt_fetched_at_et(raw: str | None) -> str:
    """Render a fetch_log timestamp (stored UTC, no tz) as ET for humans.
    Falls back to the raw value on parse failure so we never crash the
    health email over a formatting bug."""
    if not raw:
        return "n/a"
    try:
        dt = datetime.fromisoformat(str(raw).replace(" ", "T"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(ET).strftime("%Y-%m-%d %H:%M %Z")
    except (ValueError, TypeError):
        return str(raw)


async def run_health_snapshot(
    *, db: sqlite3.Connection, email: Notifier, admin_email: str,
    today: str | None = None,
) -> dict:
    """Send a daily health summary email to the admin.

    ``today`` is the ET-anchored send_date used to filter today's send
    volume. Defaults to "now in ET" — important because the server may
    run in UTC and ``date.today()`` would skew the filter around midnight
    ET (which is the exact time this job used to run, masking the bug)."""
    if today is None:
        today = datetime.now(ET).date().isoformat()

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
            f"at {_fmt_fetched_at_et(last_fetch.get('fetched_at'))}"
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
