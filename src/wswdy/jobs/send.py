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
from wswdy.repos.arrests import list_in_radius_window as list_arrests_in_radius_window
from wswdy.repos.crashes import list_in_radius_window as list_crashes_in_radius_window
from wswdy.repos.crimes import list_in_radius_window
from wswdy.repos.fetch_log import last_successful
from wswdy.repos.send_log import any_sent_today, exists_for_today, record
from wswdy.repos.subscribers import list_active, set_last_sent, tracked_districts
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
    cutoff_hour_et: int = 8,
    cutoff_minute_et: int = 10,
    stagger: bool = True,
    stagger_max_s: int = 45 * 60,
    gap_min_s: int = 30,
    gap_max_s: int = 120,
    render_static_map: Callable[..., Awaitable[Path]] | None = None,
    static_map_dir: Path = Path("./static_maps"),
) -> dict:
    """Send daily digest to all active subscribers whose tracked districts
    have completed today's report cycle.

    Per-subscriber gating: a subscriber's digest goes out as soon as
    their primary district (and any extra_districts) have shipped both
    today's crime AND arrest LISTSERV reports — OR once the cutoff
    time-of-day has passed, in which case we force-send with whatever
    we have. Subscribers in fast districts ship at 8:00; subscribers
    in slow districts ship later in the morning window or at cutoff.

    Returns: {"sent": int, "failed": int, "skipped": int, "waiting": int}.
    "waiting" counts subscribers we deliberately deferred this pass —
    they'll be re-evaluated on the next scheduler trigger.
    """
    now_et = _parse_iso_as_utc(now_iso).astimezone(ET)
    cutoff_dt = now_et.replace(
        hour=cutoff_hour_et, minute=cutoff_minute_et,
        second=0, microsecond=0,
    )
    is_at_cutoff = now_et >= cutoff_dt
    actives = list_active(db)
    log.info("Daily send: %d active subscribers", len(actives))

    mpd_warning = _is_feed_stale(db, now_iso=now_iso)
    # Per-district report tally for the day, computed once and reused
    # across the loop. Each subscriber's readiness is then evaluated
    # against just the districts they track (primary + extras).
    reports_today = reports_by_district_today(db, now_iso=now_iso)

    sent = failed = skipped = waiting = 0
    now_dt = _parse_iso_as_utc(now_iso)
    start_dt = now_dt - timedelta(hours=24)
    start_iso = start_dt.isoformat(timespec="seconds")
    end_iso = now_dt.isoformat(timespec="seconds")

    # Crashes use a rolling 7-day window because DC's crash feed lags 3-5
    # days behind real time — a 24h window would almost always be empty.
    crash_start_iso = (now_dt - timedelta(days=7)).isoformat(timespec="seconds")
    # Anything fetched into our DB in the past day counts as "newly
    # reported" for the digest header. Slight overlap is fine — better
    # to occasionally call out yesterday's late-evening fetches than to
    # miss a fresh batch.
    new_crash_cutoff_iso = (now_dt - timedelta(hours=24)).isoformat(timespec="seconds")

    if stagger:
        random.shuffle(actives)

    for i, sub in enumerate(actives):
        if exists_for_today(db, sub["id"], send_date, sub["preferred_channel"]):
            skipped += 1
            continue

        # Per-subscriber readiness: are this subscriber's tracked
        # districts done shipping today's reports? If not, only proceed
        # if we've crossed the cutoff (force-send, accept gaps).
        sub_ready = subscriber_ready_to_send(sub, reports_today)
        if not sub_ready and not is_at_cutoff:
            log.info(
                "Daily send: skipping %s (%s) — districts %s not all "
                "ready (today's kinds: %s)",
                sub["id"], sub["display_name"], tracked_districts(sub),
                {d: sorted(k) for d, k in reports_today.items()},
            )
            waiting += 1
            continue

        # Whether THIS subscriber's districts have today's arrest report —
        # drives the "No arrest report yet today" digest line when we
        # force-ship at cutoff with incomplete data.
        sub_districts = tracked_districts(sub)
        if sub_districts:
            have_arrest_today = all(
                "arrest" in reports_today.get(d, set()) for d in sub_districts
            )
        else:
            have_arrest_today = any(
                "arrest" in kinds for kinds in reports_today.values()
            )

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
        # Arrests use the same 24h window as crimes — that's
        # "yesterday's report just arrived this morning". The repo
        # excludes ungeocoded rows automatically.
        arrests = list_arrests_in_radius_window(
            db, sub["lat"], sub["lon"], sub["radius_m"],
            start=start_iso, end=end_iso,
        )
        # "Newly reported" = crashes that landed in our DB since the
        # previous daily fetch. Crash data has a 3-5 day publishing lag,
        # so this is the most actionable signal — without it the digest
        # looks identical day-to-day even when DC just published a batch.
        new_crash_count = sum(
            1 for c in crashes
            if c.get("fetched_at") and c["fetched_at"] >= new_crash_cutoff_iso
        )

        map_token = sign(hmac_secret, purpose="map", subscriber_id=sub["id"])
        unsub_token = sign(hmac_secret, purpose="unsubscribe", subscriber_id=sub["id"])
        map_url = f"{base_url}/map/{sub['id']}?token={map_token}"
        unsub_url = f"{base_url}/u/{sub['id']}?token={unsub_token}"

        text = build_digest_text(
            display_name=sub["display_name"], radius_m=sub["radius_m"],
            crimes=crimes, crashes=crashes,
            new_crash_count=new_crash_count,
            arrests=arrests, have_arrest_today=have_arrest_today,
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

    log.info(
        "Daily send done: sent=%d failed=%d skipped=%d waiting=%d",
        sent, failed, skipped, waiting,
    )
    return {
        "sent": sent, "failed": failed, "skipped": skipped, "waiting": waiting,
    }


def _is_feed_stale(db: sqlite3.Connection, *, now_iso: str) -> bool:
    """True if the last successful fetch was >24h before now_iso, or never."""
    last_ok = last_successful(db)
    if not last_ok:
        return True
    last_dt = _parse_iso_as_utc(last_ok["fetched_at"])
    now_dt = _parse_iso_as_utc(now_iso)
    return (now_dt - last_dt) > timedelta(hours=24)


def reports_by_district_today(
    db: sqlite3.Connection, *, now_iso: str,
) -> dict[str, set[str]]:
    """Return ``{district: {kinds_ingested_today}}``.

    Used by the per-subscriber readiness check to decide, for any given
    subscriber, whether their tracked districts have shipped *both*
    today's crime AND today's arrest LISTSERV report. Computed once per
    send-trigger and reused across all subscribers in that pass.
    """
    today_et_start = (
        _parse_iso_as_utc(now_iso).astimezone(ET).date()
    )
    start_str = datetime.combine(
        today_et_start, datetime.min.time(), tzinfo=ET,
    ).astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    out: dict[str, set[str]] = {}
    for d, k in db.execute(
        "SELECT DISTINCT district, kind FROM pdf_ingest_log WHERE ingested_at >= ?",
        (start_str,),
    ).fetchall():
        if not d or not k:
            continue
        out.setdefault(d.upper(), set()).add(k.lower())
    return out


def subscriber_ready_to_send(
    sub: dict, reports_today: dict[str, set[str]],
) -> bool:
    """True iff every district this subscriber tracks (primary + extras)
    has both today's crime AND today's arrest report ingested.

    Subscribers without a district fall back to the legacy global gate
    ("any crime AND any arrest in the inbox today"). That preserves
    behavior for older accounts whose district hasn't been backfilled
    yet — no surprise outages during rollout.
    """
    districts = tracked_districts(sub)
    if not districts:
        # Legacy path: any district will do.
        all_kinds: set[str] = set()
        for kinds in reports_today.values():
            all_kinds |= kinds
        return {"crime", "arrest"}.issubset(all_kinds)
    for d in districts:
        kinds = reports_today.get(d, set())
        if "crime" not in kinds or "arrest" not in kinds:
            return False
    return True


def listserv_reports_in_today(
    db: sqlite3.Connection, *, now_iso: str,
) -> tuple[bool, bool]:
    """Return (have_crime_today, have_arrest_today) for the LISTSERV
    pipeline. Used by the morning send to wait until both today's
    crime AND arrest emails have landed (per pdf_ingest_log) before
    shipping the daily digest. Without this check, the digest goes out
    before MPD's ~7:50-8:05 AM batch arrives, and arrest counts are
    always a day stale.

    Note on the comparison string: ``pdf_ingest_log.ingested_at`` is
    populated by SQLite's ``CURRENT_TIMESTAMP`` default, which writes a
    naive UTC timestamp in ``"YYYY-MM-DD HH:MM:SS"`` form (space
    separator, no offset). We must format the cutoff string the same
    way — using ``datetime.isoformat()`` would give ``"YYYY-MM-DDT..."``
    which sorts AFTER the space-separated form (' ' = 0x20 < 'T' = 0x54)
    and the WHERE clause would silently match nothing.
    """
    today_et_start = (
        _parse_iso_as_utc(now_iso).astimezone(ET).date()
    )
    start_dt_utc = datetime.combine(
        today_et_start, datetime.min.time(), tzinfo=ET,
    ).astimezone(UTC)
    start_str = start_dt_utc.strftime("%Y-%m-%d %H:%M:%S")
    rows = db.execute(
        """SELECT kind, COUNT(*) FROM pdf_ingest_log
           WHERE ingested_at >= ? GROUP BY kind""",
        (start_str,),
    ).fetchall()
    counts = {r[0]: r[1] for r in rows}
    return counts.get("crime", 0) > 0, counts.get("arrest", 0) > 0


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
    cutoff_hour_et: int = 8,
    cutoff_minute_et: int = 10,
    render_static_map: Callable[..., Awaitable[Path]] | None = None,
    static_map_dir: Path = Path("./static_maps"),
) -> dict:
    """Thin scheduler-trigger entrypoint.

    Defers the actual readiness logic to ``run_daily_sends``, which now
    decides per subscriber whether to ship (their tracked districts'
    LISTSERV reports are in) or wait (and try again next trigger). The
    only check we keep here is a global short-circuit when the API
    feed is so stale that we have nothing useful to send — anything
    we'd ship would be yesterday's data twice. In that case we still
    let the cutoff break the impasse so subscribers aren't blocked
    indefinitely on an MPD outage.
    """
    today_et = _parse_iso_as_utc(now_iso).astimezone(ET).date()
    send_date = today_et.isoformat()

    is_fresh = feed_has_yesterdays_data(db, now_iso=now_iso)
    now_et = _parse_iso_as_utc(now_iso).astimezone(ET)
    cutoff_dt = now_et.replace(
        hour=cutoff_hour_et, minute=cutoff_minute_et, second=0, microsecond=0,
    )
    is_cutoff = now_et >= cutoff_dt

    if not is_fresh and not is_cutoff:
        log.info(
            "Adaptive send: API feed stale (no yesterday data) and "
            "before cutoff (%s ET, cutoff=%02d:%02d ET) — waiting",
            now_et.strftime("%H:%M"), cutoff_hour_et, cutoff_minute_et,
        )
        return {"status": "waiting_for_fresh_feed", "send_date": send_date}

    counts = await run_daily_sends(
        db=db, email=email, whatsapp=whatsapp, alerter=alerter,
        base_url=base_url, hmac_secret=hmac_secret,
        send_date=send_date, now_iso=now_iso,
        cutoff_hour_et=cutoff_hour_et, cutoff_minute_et=cutoff_minute_et,
        stagger=True,
        render_static_map=render_static_map,
        static_map_dir=static_map_dir,
    )
    if counts["sent"] == 0 and counts["waiting"] > 0:
        return {"status": "waiting_for_districts", "send_date": send_date,
                **counts}
    return {
        "status": "sent" if is_fresh else "sent_at_cutoff",
        "send_date": send_date,
        **counts,
    }
