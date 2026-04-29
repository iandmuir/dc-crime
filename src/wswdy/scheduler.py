"""APScheduler setup.

Three daily ET-anchored jobs:

  prune     03:00       cleans old data
  send      06:00-19:00 hourly — adaptive: tries to ship the daily digest
                               every hour until either yesterday's MPD
                               batch lands or we hit the 7 PM cutoff.
                               Each invocation also runs a fresh fetch.
  health    23:00       end-of-day snapshot

The standalone fetch job is gone: every hourly send trigger fetches first,
which both replaces the morning fetch and gives the freshness check the
freshest possible view of the feed.
"""
from collections.abc import Awaitable, Callable
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

ET = ZoneInfo("America/New_York")
JOB_IDS = ("prune", "send", "health")


def build_scheduler(
    *, fetch_fn: Callable[[], Awaitable[None]],  # noqa: ARG001 — kept for API compat
    send_fn: Callable[[], Awaitable[None]],
    prune_fn: Callable[[], Awaitable[None]],
    health_fn: Callable[[], Awaitable[None]],
) -> AsyncIOScheduler:
    s = AsyncIOScheduler(timezone=ET)
    s.add_job(prune_fn, CronTrigger(hour=3, minute=0, timezone=ET), id="prune")
    s.add_job(
        send_fn,
        CronTrigger(hour="6-19", minute=0, timezone=ET),
        id="send",
    )
    s.add_job(health_fn, CronTrigger(hour=23, minute=0, timezone=ET), id="health")
    return s
