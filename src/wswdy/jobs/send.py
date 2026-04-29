"""Daily digest send job (staggered)."""
import asyncio
import logging
import random
import sqlite3
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path


def _parse_iso_as_utc(s: str) -> datetime:
    """Parse an ISO timestamp; treat naive (no tz) values as UTC.

    fetch_log.fetched_at is stored by SQLite's CURRENT_TIMESTAMP as
    'YYYY-MM-DD HH:MM:SS' (no offset) — and SQLite's CURRENT_TIMESTAMP
    is always UTC. now_iso from the scheduler is timezone-aware
    (datetime.now(UTC).isoformat()). Comparing the two without
    normalizing throws TypeError.
    """
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt

from wswdy.alerts import AdminAlerter
from wswdy.digest import build_digest_text
from wswdy.notifiers.base import Notifier, dispatch
from wswdy.repos.crashes import list_in_radius_window as list_crashes_in_radius_window
from wswdy.repos.crimes import list_in_radius_window
from wswdy.repos.fetch_log import last_successful
from wswdy.repos.send_log import any_sent_today, exists_for_today, record
from wswdy.repos.subscribers import list_active, set_last_sent
from wswdy.tiers import classify
from wswdy.timefmt import ET
from wswdy.tokens import sign

log = logging.getLogger(__name__)


async def run_daily_sends(
    *,
    db: sqlite3.Connection,
    email: Notifier,
    whatsapp: Notifier,
    alerter: AdminAlerter,
    base_url: str,
    hmac_secret: str,
    send_date: str,
    now_iso: str,
    stagger: bool = True,
    stagger_max_s: int = 45 * 60,
    gap_min_s: int = 30,
    gap_max_s: int = 120,
    render_static_map: Callable[..., Awaitable[Path]] | None = None,
    static_map_dir: Path = Path("./static_maps"),
) -> dict:
    """Send daily digest to all active subscribers.

    Returns: {"sent": int, "failed": int, "skipped": int}
    """
    actives = list_active(db)
    log.info("Daily send: %d active subscribers", len(actives))

    mpd_warning = _is_feed_stale(db, now_iso=now_iso)

    sent = failed = skipped = 0
    now_dt = _parse_iso_as_utc(now_iso)
    start_dt = now_dt - timedelta(hours=24)
    start_iso = start_dt.isoformat(timespec="seconds")
    end_iso = now_dt.isoformat(timespec="seconds")

    # Crashes use a rolling 7-day window because DC's crash feed lags 3-5
    # days behind real time — a 24h window would almost always be empty.
    crash_start_iso = (now_dt - timedelta(days=7)).isoformat(timespec="seconds")

    if stagger:
        random.shuffle(actives)

    for i, sub in enumerate(actives):
        if exists_for_today(db, sub["id"], send_date, sub["preferred_channel"]):
            skipped += 1
            continue

        if stagger and i > 0:
            await asyncio.sleep(random.uniform(gap_min_s, gap_max_s))

        crimes = list_in_radius_window(
            db, sub["lat"], sub["lon"], sub["radius_m"],
            start=start_iso, end=end_iso,
        )
        crashes = list_crashes_in_radius_window(
            db, sub["lat"], sub["lon"], sub["radius_m"],
            start=crash_start_iso, end=end_iso,
        )

        map_token = sign(hmac_secret, purpose="map", subscriber_id=sub["id"])
        unsub_token = sign(hmac_secret, purpose="unsubscribe", subscriber_id=sub["id"])
        map_url = f"{base_url}/map/{sub['id']}?token={map_token}"
        unsub_url = f"{base_url}/u/{sub['id']}?token={unsub_token}"

        text = build_digest_text(
            display_name=sub["display_name"], radius_m=sub["radius_m"],
            crimes=crimes, crashes=crashes,
            home_lat=sub["lat"], home_lon=sub["lon"],
            map_url=map_url, unsubscribe_url=unsub_url,
            mpd_warning=mpd_warning,
        )

        # Static map preview (best-effort; still send text-only on render failure)
        image_path: Path | None = None
        if render_static_map is not None:
            try:
                image_path = await render_static_map(
                    center_lat=sub["lat"], center_lon=sub["lon"],
                    radius_m=sub["radius_m"],
                    markers=[(c["lat"], c["lon"], classify(c["offense"], c.get("method")))
                             for c in crimes],
                    out_path=static_map_dir / f"{sub['id']}_{send_date}.png",
                )
            except Exception as e:
                log.warning("Static map render failed for %s: %s", sub["id"], e)

        result = await dispatch(
            sub,
            email_notifier=email,
            whatsapp_notifier=whatsapp,
            subject=f"WTFDC for {sub['display_name']} — {send_date}",
            text=text,
            image_path=image_path,
            unsubscribe_url=unsub_url,
        )

        if result.ok:
            record(db, sub["id"], send_date, sub["preferred_channel"], "sent")
            set_last_sent(db, sub["id"], now_iso)
            sent += 1
        else:
            record(
                db, sub["id"], send_date, sub["preferred_channel"], "failed",
                error=f"{result.error}: {result.detail or ''}",
            )
            failed += 1
            if result.error == "session_expired":
                await alerter.alert(
                    alert_type="whatsapp_session_expired",
                    message="WhatsApp MCP session expired — re-link the device.",
                )

    log.info("Daily send done: sent=%d failed=%d skipped=%d", sent, failed, skipped)
    return {"sent": sent, "failed": failed, "skipped": skipped}


def _is_feed_stale(db: sqlite3.Connection, *, now_iso: str) -> bool:
    """True if the last successful fetch was >24h before now_iso, or never."""
    last_ok = last_successful(db)
    if not last_ok:
        return True
    last_dt = _parse_iso_as_utc(last_ok["fetched_at"])
    now_dt = _parse_iso_as_utc(now_iso)
    return (now_dt - last_dt) > timedelta(hours=24)


def feed_has_yesterdays_data(
    db: sqlite3.Connection, *, now_iso: str, min_records: int = 5,
) -> bool:
    """True if the MPD feed has been updated with at least `min_records` reports
    from yesterday's ET calendar date.

    This is the trigger for the adaptive send: MPD's daily publishing batch
    is what causes yesterday's records to land in the feed. Until that
    happens, sending out a digest about "yesterday" makes promises the data
    can't keep. We wait until the batch lands (any reports from yesterday's
    date) and ship as soon as it does.

    A small min_records threshold (5) avoids triggering on a single straggler
    report; DC averages ~80-100 reports per day, so 5 is a safe "the batch
    has clearly started landing" signal.
    """
    now_et = _parse_iso_as_utc(now_iso).astimezone(ET)
    yesterday_et = now_et.date() - timedelta(days=1)
    # Bounds: yesterday 00:00 ET -> today 00:00 ET, expressed as ISO UTC
    # strings comparable to the report_dt column.
    start = datetime.combine(yesterday_et, datetime.min.time(), tzinfo=ET)
    end = datetime.combine(now_et.date(), datetime.min.time(), tzinfo=ET)
    row = db.execute(
        "SELECT COUNT(*) FROM crimes WHERE report_dt >= ? AND report_dt < ?",
        (start.astimezone(UTC).isoformat(), end.astimezone(UTC).isoformat()),
    ).fetchone()
    return (row[0] or 0) >= min_records


async def run_send_if_ready(
    *,
    db: sqlite3.Connection,
    email: Notifier,
    whatsapp: Notifier,
    alerter: AdminAlerter,
    base_url: str,
    hmac_secret: str,
    now_iso: str,
    cutoff_hour_et: int = 19,
    render_static_map: Callable[..., Awaitable[Path]] | None = None,
    static_map_dir: Path = Path("./static_maps"),
) -> dict:
    """Adaptive daily send. Runs cheaply on every hourly trigger; only fires
    the actual digest send when:

      - we haven't already sent today AND
      - (the MPD feed contains yesterday's data OR we're past the cutoff hour)

    The cutoff is the "force-send no later than" guard for days when MPD
    publishes late or not at all.

    Returns a status dict; the {sent, failed, skipped} counts are present
    only when an actual send fired.
    """
    today_et = _parse_iso_as_utc(now_iso).astimezone(ET).date()
    send_date = today_et.isoformat()

    if any_sent_today(db, send_date):
        log.info("Adaptive send: already sent today (%s), skipping", send_date)
        return {"status": "already_sent_today", "send_date": send_date}

    is_fresh = feed_has_yesterdays_data(db, now_iso=now_iso)
    now_hour_et = _parse_iso_as_utc(now_iso).astimezone(ET).hour
    is_cutoff = now_hour_et >= cutoff_hour_et

    if not is_fresh and not is_cutoff:
        log.info(
            "Adaptive send: feed not fresh yet (no yesterday data) and "
            "before cutoff (%dh ET, cutoff=%dh) — waiting for next trigger",
            now_hour_et, cutoff_hour_et,
        )
        return {"status": "waiting_for_fresh_data", "send_date": send_date}

    log.info(
        "Adaptive send: dispatching (fresh=%s, cutoff=%s)",
        is_fresh, is_cutoff,
    )
    counts = await run_daily_sends(
        db=db, email=email, whatsapp=whatsapp, alerter=alerter,
        base_url=base_url, hmac_secret=hmac_secret,
        send_date=send_date, now_iso=now_iso,
        stagger=True,
        render_static_map=render_static_map,
        static_map_dir=static_map_dir,
    )
    return {
        "status": "sent" if is_fresh else "sent_at_cutoff",
        "send_date": send_date,
        **counts,
    }
