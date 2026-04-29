import pytest

from wswdy.tokens import TokenError, sign, verify

SECRET = "test-secret-32-bytes-long-base64ish"


def test_roundtrip_no_expiry():
    t = sign(SECRET, purpose="unsubscribe", subscriber_id="abc123")
    payload = verify(SECRET, t, purpose="unsubscribe")
    assert payload["subscriber_id"] == "abc123"


def test_roundtrip_with_expiry():
    t = sign(SECRET, purpose="approve", subscriber_id="abc", ttl_seconds=60)
    payload = verify(SECRET, t, purpose="approve")
    assert payload["subscriber_id"] == "abc"


def test_expired_token_rejected():
    t = sign(SECRET, purpose="approve", subscriber_id="abc", ttl_seconds=-1)
    with pytest.raises(TokenError, match="expired"):
        verify(SECRET, t, purpose="approve")


def test_wrong_purpose_rejected():
    t = sign(SECRET, purpose="approve", subscriber_id="abc")
    with pytest.raises(TokenError, match="purpose"):
        verify(SECRET, t, purpose="unsubscribe")


def test_tampered_token_rejected():
    t = sign(SECRET, purpose="map", subscriber_id="abc")
    head, sig = t.split(".")
    tampered = head + "X" + "." + sig
    with pytest.raises(TokenError):
        verify(SECRET, tampered, purpose="map")


def test_wrong_secret_rejected():
    t = sign(SECRET, purpose="map", subscriber_id="abc")
    with pytest.raises(TokenError):
        verify("different-secret", t, purpose="map")


def test_garbage_token_rejected():
    with pytest.raises(TokenError):
        verify(SECRET, "not.even.close", purpose="map")
