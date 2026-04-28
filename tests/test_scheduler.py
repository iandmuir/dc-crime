from unittest.mock import AsyncMock

from apscheduler.triggers.cron import CronTrigger

from wswdy.scheduler import JOB_IDS, build_scheduler


def test_build_scheduler_registers_all_jobs():
    fetch = AsyncMock()
    send = AsyncMock()
    prune = AsyncMock()
    health = AsyncMock()
    s = build_scheduler(fetch_fn=fetch, send_fn=send, prune_fn=prune, health_fn=health)
    job_ids = {j.id for j in s.get_jobs()}
    assert job_ids == set(JOB_IDS)


def test_jobs_use_eastern_time():
    fetch = AsyncMock()
    send = AsyncMock()
    prune = AsyncMock()
    health = AsyncMock()
    s = build_scheduler(fetch_fn=fetch, send_fn=send, prune_fn=prune, health_fn=health)
    for j in s.get_jobs():
        assert isinstance(j.trigger, CronTrigger)
        # zoneinfo timezone string
        assert "New_York" in str(j.trigger.timezone)


def test_jobs_have_expected_times():
    fetch = AsyncMock()
    send = AsyncMock()
    prune = AsyncMock()
    health = AsyncMock()
    s = build_scheduler(fetch_fn=fetch, send_fn=send, prune_fn=prune, health_fn=health)
    times = {j.id: str(j.trigger) for j in s.get_jobs()}
    assert "hour='3'" in times["prune"]
    assert "hour='5'" in times["fetch"] and "minute='30'" in times["fetch"]
    assert "hour='6'" in times["send"]
    assert "hour='23'" in times["health"]
