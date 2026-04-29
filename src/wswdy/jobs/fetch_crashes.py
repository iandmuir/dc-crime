"""DC crashes feed fetch job.

Sibling to jobs/fetch.py (which pulls MPD crime). Crashes have their own
upstream (DCGIS_DATA Public_Safety_WebMercator/24) and a longer publishing
lag (3-5 days from incident to feed appearance). We fetch a 30-day rolling
window and upsert by id, so re-fetches refresh stale records cheaply.

Failures are logged but don't alert — crashes are a "context" feature in
the digest, so a missed fetch degrades gracefully (we just show stale or
no crash data, and the rest of the briefing still ships).
"""
import logging
import sqlite3

from wswdy.clients.dc_crashes import DEFAULT_LOOKBACK_DAYS, fetch_recent_crashes
from wswdy.repos.crashes import upsert_many

log = logging.getLogger(__name__)


async def run_crash_fetch(
    *,
    db: sqlite3.Connection,
    feed_url: str | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> dict:
    """Fetch the crashes feed and upsert into the crashes table.

    Returns: {"status": "ok"|"failed", "added": int, "updated": int, "error"?: str}
    """
    try:
        kwargs = {"lookback_days": lookback_days}
        if feed_url:
            kwargs["feed_url"] = feed_url
        records = await fetch_recent_crashes(**kwargs)
        added, updated = upsert_many(db, records)
        log.info("crashes fetch ok: +%d / ~%d (%d-day window)",
                 added, updated, lookback_days)
        return {"status": "ok", "added": added, "updated": updated}
    except Exception as e:
        log.warning("crashes fetch failed: %s", e)
        return {"status": "failed", "error": str(e)}
