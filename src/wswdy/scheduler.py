"""APScheduler setup.

Daily ET-anchored jobs:

  send      06:00-19:00 hourly — adaptive: tries to ship the daily digest
                               every hour until either yesterday's MPD
                               batch lands or we hit the 7 PM cutoff.
                               Each invocation also runs a fresh fetch.
  inbound   every 5 min — scans the WhatsApp bridge DB for "STOP" replies
                          and unsubscribes the matching subscribers.
  health    23:00       end-of-day snapshot

The standalone fetch job is gone: every hourly send trigger fetches first,
which both replaces the morning fetch and gives the freshness check the
freshest possible view of the feed.

There is no prune job — we deliberately retain crime / crash / arrest
history indefinitely so the map can support arbitrary lookback windows
in the future. SQLite handles years of DC-volume data trivially.
"""
from collections.abc import Awaitable, Callable
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

ET = ZoneInfo("America/New_York")
JOB_IDS = ("send", "inbound", "health")


def build_scheduler(
    *, fetch_fn: Callable[[], Awaitable[None]],  # noqa: ARG001 — kept for API compat
    send_fn: Callable[[], Awaitable[None]],
    health_fn: Callable[[], Awaitable[None]],
    inbound_fn: Callable[[], Awaitable[None]] | None = None,
) -> AsyncIOScheduler:
    s = AsyncIOScheduler(timezone=ET)
    s.add_job(
        send_fn,
        CronTrigger(hour="6-19", minute=0, timezone=ET),
        id="send",
    )
    if inbound_fn is not None:
        s.add_job(inbound_fn, IntervalTrigger(minutes=5), id="inbound")
    s.add_job(health_fn, CronTrigger(hour=23, minute=0, timezone=ET), id="health")
    return s
