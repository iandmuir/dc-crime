"""Daily digest send job (staggered)."""
import asyncio
import logging
import random
import sqlite3
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from pathlib import Path

from wswdy.alerts import AdminAlerter
from wswdy.digest import build_digest_text
from wswdy.notifiers.base import Notifier, dispatch
from wswdy.repos.crimes import list_in_radius_window
from wswdy.repos.fetch_log import last_successful
from wswdy.repos.send_log import exists_for_today, record
from wswdy.repos.subscribers import list_active, set_last_sent
from wswdy.tiers import classify
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
    start_dt = datetime.fromisoformat(now_iso) - timedelta(hours=24)
    start_iso = start_dt.isoformat(timespec="seconds")
    end_iso = now_iso

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

        map_token = sign(hmac_secret, purpose="map", subscriber_id=sub["id"])
        unsub_token = sign(hmac_secret, purpose="unsubscribe", subscriber_id=sub["id"])
        map_url = f"{base_url}/map/{sub['id']}?token={map_token}"
        unsub_url = f"{base_url}/u/{sub['id']}?token={unsub_token}"

        text = build_digest_text(
            display_name=sub["display_name"], radius_m=sub["radius_m"],
            crimes=crimes, home_lat=sub["lat"], home_lon=sub["lon"],
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
            subject=f"DC briefing for {sub['display_name']} — {send_date}",
            text=text,
            image_path=image_path,
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
    last_dt = datetime.fromisoformat(last_ok["fetched_at"].replace("Z", "+00:00"))
    now_dt = datetime.fromisoformat(now_iso)
    return (now_dt - last_dt) > timedelta(hours=24)
