"""APScheduler setup — registers the four daily jobs in ET."""
from collections.abc import Awaitable, Callable
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

ET = ZoneInfo("America/New_York")
JOB_IDS = ("prune", "fetch", "send", "health")


def build_scheduler(
    *, fetch_fn: Callable[[], Awaitable[None]],
    send_fn: Callable[[], Awaitable[None]],
    prune_fn: Callable[[], Awaitable[None]],
    health_fn: Callable[[], Awaitable[None]],
) -> AsyncIOScheduler:
    s = AsyncIOScheduler(timezone=ET)
    s.add_job(prune_fn, CronTrigger(hour=3, minute=0, timezone=ET), id="prune")
    s.add_job(fetch_fn, CronTrigger(hour=5, minute=30, timezone=ET), id="fetch")
    s.add_job(send_fn,  CronTrigger(hour=6, minute=0, timezone=ET), id="send")
    s.add_job(health_fn, CronTrigger(hour=23, minute=0, timezone=ET), id="health")
    return s
