import time

from wswdy.ratelimit import RateLimiter


def test_under_limit_allowed():
    rl = RateLimiter(max_requests=3, window_s=60)
    assert rl.check("1.2.3.4") is True
    assert rl.check("1.2.3.4") is True
    assert rl.check("1.2.3.4") is True


def test_over_limit_rejected():
    rl = RateLimiter(max_requests=2, window_s=60)
    rl.check("1.2.3.4")
    rl.check("1.2.3.4")
    assert rl.check("1.2.3.4") is False


def test_window_resets():
    rl = RateLimiter(max_requests=1, window_s=1)
    assert rl.check("a") is True
    assert rl.check("a") is False
    time.sleep(1.1)
    assert rl.check("a") is True


def test_per_ip_isolation():
    rl = RateLimiter(max_requests=1, window_s=60)
    assert rl.check("a") is True
    assert rl.check("a") is False
    assert rl.check("b") is True
