"""Daily MPD GeoJSON fetch job."""
import asyncio
import json
import logging
import sqlite3
from pathlib import Path

from wswdy.alerts import AdminAlerter
from wswdy.clients.mpd import fetch_recent_geojson, parse_features
from wswdy.repos.crimes import upsert_many
from wswdy.repos.fetch_log import record_failure, record_success

log = logging.getLogger(__name__)

DEFAULT_RETRY_DELAYS_S = [300, 900, 2700]  # 5, 15, 45 minutes


async def run_fetch(
    *,
    db: sqlite3.Connection,
    feed_url: str,
    alerter: AdminAlerter,
    fixture_path: str | None = None,
    retry_delays_s: list[int] | None = None,
) -> dict:
    """Fetches the MPD feed (with retries) and upserts into the crimes table.

    Returns: {"status": "ok"|"failed", "added": int, "updated": int, "error"?: str}
    """
    if fixture_path:
        log.info("Using fixture instead of live feed: %s", fixture_path)
        try:
            data = json.loads(Path(fixture_path).read_text())
            features = parse_features(data)
            added, updated = upsert_many(db, features)
            record_success(db, added=added, updated=updated)
            return {"status": "ok", "added": added, "updated": updated}
        except Exception as e:
            record_failure(db, error=str(e))
            await alerter.alert(alert_type="mpd_down", message=f"fixture load failed: {e}")
            return {"status": "failed", "error": str(e)}

    delays = retry_delays_s if retry_delays_s is not None else DEFAULT_RETRY_DELAYS_S
    last_error: str | None = None
    for attempt, delay in enumerate([0, *delays]):
        if delay:
            log.info("Retrying MPD fetch in %ds (attempt %d)", delay, attempt + 1)
            await asyncio.sleep(delay)
        try:
            data = await fetch_recent_geojson(feed_url)
            features = parse_features(data)
            added, updated = upsert_many(db, features)
            record_success(db, added=added, updated=updated)
            log.info("MPD fetch ok: +%d / ~%d", added, updated)
            return {"status": "ok", "added": added, "updated": updated}
        except Exception as e:
            last_error = str(e)
            log.warning("MPD fetch attempt %d failed: %s", attempt + 1, e)

    record_failure(db, error=last_error or "unknown")
    await alerter.alert(
        alert_type="mpd_down",
        message=f"MPD feed unreachable after {len(delays) + 1} attempts: {last_error}",
    )
    return {"status": "failed", "error": last_error}
