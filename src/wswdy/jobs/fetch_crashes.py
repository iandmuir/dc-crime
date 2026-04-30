"""DC crashes feed fetch job.

Sibling to jobs/fetch.py (which pulls MPD crime). Crashes have their own
upstream (DCGIS_DATA Public_Safety_WebMercator/24) and a longer publishing
lag (3-5 days from incident to feed appearance). We fetch a 30-day rolling
window and upsert by id, so re-fetches refresh stale records cheaply.

After upserting crashes, we also fetch the *parties* (drivers / passengers
/ pedestrians / cyclists) for those crashes from layer 25, joined by
CRIMEID. Parties land in the separate crash_parties table.

Failures are logged but don't alert — crashes are a "context" feature in
the digest, so a missed fetch degrades gracefully (we just show stale or
no crash data, and the rest of the briefing still ships).
"""
import logging
import sqlite3

from wswdy.clients.dc_crash_details import fetch_parties_for_crashes
from wswdy.clients.dc_crashes import DEFAULT_LOOKBACK_DAYS, fetch_recent_crashes
from wswdy.repos.crash_parties import upsert_many as upsert_parties
from wswdy.repos.crashes import upsert_many

log = logging.getLogger(__name__)


async def run_crash_fetch(
    *,
    db: sqlite3.Connection,
    feed_url: str | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> dict:
    """Fetch crashes + their parties and upsert both.

    Returns: {"status": ..., "added": int, "updated": int,
              "parties_added": int, "parties_updated": int, "error"?: str}
    """
    try:
        kwargs = {"lookback_days": lookback_days}
        if feed_url:
            kwargs["feed_url"] = feed_url
        records = await fetch_recent_crashes(**kwargs)
        added, updated = upsert_many(db, records)
        log.info("crashes fetch ok: +%d / ~%d (%d-day window)",
                 added, updated, lookback_days)
    except Exception as e:
        log.warning("crashes fetch failed: %s", e)
        return {"status": "failed", "error": str(e)}

    # Fetch parties for the crashes we just upserted. Use the IDs we just
    # parsed (rather than re-querying the DB) since they're in memory.
    crimeids = [r["id"] for r in records if r.get("id")]
    parties_added = parties_updated = 0
    try:
        parties = await fetch_parties_for_crashes(crimeids=crimeids)
        parties_added, parties_updated = upsert_parties(db, parties)
        log.info("crash parties fetch ok: +%d / ~%d (%d crashes)",
                 parties_added, parties_updated, len(crimeids))
    except Exception as e:
        log.warning("crash parties fetch failed: %s", e)
        # Don't return failure — main crash data is fine, popups just won't
        # have party detail until next fetch succeeds.

    return {
        "status": "ok",
        "added": added,
        "updated": updated,
        "parties_added": parties_added,
        "parties_updated": parties_updated,
    }
