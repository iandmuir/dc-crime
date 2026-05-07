"""APScheduler setup.

Daily ET-anchored jobs:

  send      06:00-19:00 hourly — adaptive: tries to ship the daily digest
                               every hour until either yesterday's MPD
                               batch lands or we hit the 7 PM cutoff.
                               Each invocation also runs a fresh fetch.
  inbound   every 5 min — scans the WhatsApp bridge DB for "STOP" replies
                          and unsubscribes the matching subscribers.
  health    08:30       morning snapshot — runs right after the morning
                        send window closes (last attempt at 8:15) so
                        the email is a same-day "did the digests ship?"
                        check rather than a stale end-of-day report.

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
JOB_IDS = (
    "send_morning_a", "send_morning_b", "send_fallback", "inbound", "health",
)


def build_scheduler(
    *, fetch_fn: Callable[[], Awaitable[None]],  # noqa: ARG001 — kept for API compat
    send_fn: Callable[[], Awaitable[None]],
    health_fn: Callable[[], Awaitable[None]],
    inbound_fn: Callable[[], Awaitable[None]] | None = None,
) -> AsyncIOScheduler:
    s = AsyncIOScheduler(timezone=ET)
    # Morning window — run every 5 min between 7:00 and 8:10 ET so we can
    # ship the digest as soon as today's MPD LISTSERV batch arrives (crime
    # ~7:50, arrest ~8:00-8:05). The send job itself is idempotent; once
    # it sees today's data is in (or hits the cutoff), it ships and any
    # subsequent triggers no-op via the "already sent today" guard.
    s.add_job(
        send_fn,
        CronTrigger(hour="7", minute="*/5", timezone=ET),
        id="send_morning_a",
    )
    s.add_job(
        send_fn,
        CronTrigger(hour="8", minute="0,5,10,15", timezone=ET),
        id="send_morning_b",
    )
    # Hourly fallback later in the day in case morning never shipped
    # (MPD outage, our service down, etc) — keeps the original "every
    # subscriber gets *something* by EoD" guarantee.
    s.add_job(
        send_fn,
        CronTrigger(hour="9-19", minute=0, timezone=ET),
        id="send_fallback",
    )
    if inbound_fn is not None:
        # Every minute — the scan is cheap (a couple of SQL queries) and
        # subscribers expect their STOP reply to take effect immediately,
        # not after a five-minute wait.
        s.add_job(inbound_fn, IntervalTrigger(minutes=1), id="inbound")
    # Morning snapshot — run shortly after the last morning send attempt
    # (8:15 ET) so the "Sends today" line reflects today's actual run.
    s.add_job(health_fn, CronTrigger(hour=8, minute=30, timezone=ET), id="health")
    return s
