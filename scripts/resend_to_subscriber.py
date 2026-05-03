#!/usr/bin/env python3
"""Manually re-dispatch today's daily digest to a single subscriber.

Used when a subscriber's row in ``send_log`` says we already sent today
but the message didn't actually reach them — e.g. K's WhatsApp message
silently dropped because her phone number was missing the country code.

The script:
  1. Looks the subscriber up by id, phone, or email substring.
  2. Deletes their ``send_log`` row for today (so the per-subscriber
     ``exists_for_today`` guard returns False).
  3. Calls ``run_daily_sends`` directly. Other subscribers' rows for
     today are still in place, so they'll be skipped — only the target
     subscriber gets a re-send.

Usage::

    python scripts/resend_to_subscriber.py <id-or-phone-or-email-fragment>
    python scripts/resend_to_subscriber.py +19174945082
    python scripts/resend_to_subscriber.py "kate@gmail"

Run on the server::

    /root/wswdy/.venv/bin/python scripts/resend_to_subscriber.py +19174945082
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, date, datetime
from pathlib import Path

# Make the source tree importable when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wswdy.alerts import AdminAlerter  # noqa: E402
from wswdy.clients.geoapify import render_static_map  # noqa: E402
from wswdy.config import get_settings  # noqa: E402
from wswdy.db import connect, init_schema  # noqa: E402
from wswdy.jobs.send import run_daily_sends  # noqa: E402
from wswdy.notifiers.email import EmailNotifier  # noqa: E402
from wswdy.notifiers.whatsapp import WhatsAppMcpNotifier  # noqa: E402


def _find_subscriber(db, query: str) -> dict | None:
    """Match by id, phone (E.164), or email-substring (case-insensitive)."""
    db.row_factory = __import__("sqlite3").Row
    # Exact id match
    row = db.execute("SELECT * FROM subscribers WHERE id=?", (query,)).fetchone()
    if row:
        return dict(row)
    # Exact phone match (assumes already E.164)
    row = db.execute("SELECT * FROM subscribers WHERE phone=?", (query,)).fetchone()
    if row:
        return dict(row)
    # Email or display-name substring (handy for human queries)
    rows = db.execute(
        "SELECT * FROM subscribers WHERE LOWER(email) LIKE ? "
        "OR LOWER(display_name) LIKE ?",
        (f"%{query.lower()}%", f"%{query.lower()}%"),
    ).fetchall()
    if len(rows) == 1:
        return dict(rows[0])
    if len(rows) > 1:
        print(f"warning: {len(rows)} subscribers match {query!r}; "
              f"refine to a unique value.", file=sys.stderr)
        for r in rows:
            print(f"  id={r['id']} name={r['display_name']!r} "
                  f"email={r['email']!r} phone={r['phone']!r}",
                  file=sys.stderr)
    return None


async def _run(query: str) -> int:
    settings = get_settings()
    db = connect(settings.db_path)
    init_schema(db)

    sub = _find_subscriber(db, query)
    if not sub:
        print(f"no subscriber found for {query!r}", file=sys.stderr)
        return 1
    if sub["status"] != "APPROVED":
        print(f"subscriber {sub['id']} is {sub['status']!r}, not APPROVED — "
              f"refusing to send", file=sys.stderr)
        return 1
    print(f"target: {sub['display_name']} ({sub['preferred_channel']}, "
          f"{sub.get('email') or sub.get('phone')})")

    today = str(date.today())
    deleted = db.execute(
        "DELETE FROM send_log WHERE subscriber_id=? AND send_date=?",
        (sub["id"], today),
    ).rowcount
    db.commit()
    print(f"cleared {deleted} send_log row(s) for today")

    # Wire up the same dependencies main.py builds for the scheduler job.
    email_notifier = EmailNotifier(
        host=settings.smtp_host, port=settings.smtp_port,
        user=settings.smtp_user, password=settings.smtp_pass,
        sender=settings.smtp_from,
    )
    whatsapp_notifier = WhatsAppMcpNotifier(
        base_url=settings.whatsapp_mcp_url, token=settings.whatsapp_mcp_token,
    )
    alerter = AdminAlerter(
        db=db, email=email_notifier,
        admin_email=settings.admin_email,
        ha_webhook_url=settings.ha_webhook_url,
    )

    static_map_dir = Path(
        settings.static_map_dir or f"{settings.log_dir}/static_maps"
    )

    async def render(*, center_lat, center_lon, radius_m, markers, out_path):
        return await render_static_map(
            api_key=settings.geoapify_api_key,
            center_lat=center_lat, center_lon=center_lon,
            radius_m=radius_m, markers=markers, out_path=out_path,
        )

    # run_daily_sends iterates list_active() and per-subscriber checks
    # exists_for_today. With the target's row cleared and everyone else's
    # in place, only the target gets dispatched.
    now_iso = datetime.now(UTC).isoformat(timespec="seconds")
    counts = await run_daily_sends(
        db=db, email=email_notifier, whatsapp=whatsapp_notifier,
        alerter=alerter, base_url=settings.base_url,
        hmac_secret=settings.hmac_secret,
        send_date=today, now_iso=now_iso,
        stagger=False,  # only one recipient — no need to spread
        render_static_map=render, static_map_dir=static_map_dir,
    )
    print(f"result: {counts}")
    return 0 if counts.get("sent", 0) >= 1 else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "query",
        help="Subscriber id, phone (E.164), or email/name substring.",
    )
    args = ap.parse_args()
    return asyncio.run(_run(args.query))


if __name__ == "__main__":
    raise SystemExit(main())
