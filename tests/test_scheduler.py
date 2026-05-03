from unittest.mock import AsyncMock

from apscheduler.triggers.cron import CronTrigger

from wswdy.scheduler import JOB_IDS, build_scheduler


def test_build_scheduler_registers_all_jobs():
    fetch = AsyncMock()
    send = AsyncMock()
    health = AsyncMock()
    s = build_scheduler(fetch_fn=fetch, send_fn=send, health_fn=health)
    job_ids = {j.id for j in s.get_jobs()}
    assert job_ids == set(JOB_IDS) - {"inbound"}  # inbound only when configured


def test_jobs_use_eastern_time():
    fetch = AsyncMock()
    send = AsyncMock()
    health = AsyncMock()
    s = build_scheduler(fetch_fn=fetch, send_fn=send, health_fn=health)
    for j in s.get_jobs():
        assert isinstance(j.trigger, CronTrigger)
        # zoneinfo timezone string
        assert "New_York" in str(j.trigger.timezone)


def test_jobs_have_expected_times():
    fetch = AsyncMock()
    send = AsyncMock()
    health = AsyncMock()
    s = build_scheduler(fetch_fn=fetch, send_fn=send, health_fn=health)
    times = {j.id: str(j.trigger) for j in s.get_jobs()}
    # No prune anymore — we retain history indefinitely.
    assert "prune" not in times
    # Morning window: 7 AM every 5 minutes, then 8 AM through :15
    assert "hour='7'" in times["send_morning_a"]
    assert "*/5" in times["send_morning_a"]
    assert "hour='8'" in times["send_morning_b"]
    # Hourly fallback resumes at 9 AM through 7 PM
    assert "hour='9-19'" in times["send_fallback"]
    assert "hour='23'" in times["health"]
