"""Tests for phone number normalization."""
import pytest

from wswdy.phone import InvalidPhoneNumber, normalize_phone


# ---- happy path ----

@pytest.mark.parametrize("inp,expected", [
    # Already E.164
    ("+12024681234",         "+12024681234"),
    ("+1 202 468 1234",      "+12024681234"),
    ("+1 (202) 468-1234",    "+12024681234"),
    ("+44 20 7946 0958",     "+442079460958"),
    # 10-digit US — get +1 prepended (the K case)
    ("9174945082",           "+19174945082"),
    ("(202) 468-1234",       "+12024681234"),
    ("202-468-1234",         "+12024681234"),
    ("202.468.1234",         "+12024681234"),
    # 11-digit starting with 1 — get + prepended
    ("12024681234",          "+12024681234"),
    ("1-202-468-1234",       "+12024681234"),
])
def test_normalize_phone_valid(inp, expected):
    assert normalize_phone(inp) == expected


# ---- error path ----

def test_normalize_phone_rejects_empty():
    with pytest.raises(InvalidPhoneNumber, match="empty"):
        normalize_phone("")


def test_normalize_phone_rejects_none():
    with pytest.raises(InvalidPhoneNumber):
        normalize_phone(None)


def test_normalize_phone_rejects_no_digits():
    with pytest.raises(InvalidPhoneNumber, match="no digits"):
        normalize_phone("call me!")


def test_normalize_phone_rejects_too_short():
    """8-digit numbers are ambiguous (e.g. partial entry); reject."""
    with pytest.raises(InvalidPhoneNumber):
        normalize_phone("12345678")


def test_normalize_phone_rejects_too_long_e164():
    with pytest.raises(InvalidPhoneNumber, match="length"):
        normalize_phone("+1234567890123456")  # 16 digits — over E.164 max


def test_normalize_phone_rejects_garbage_with_plus():
    """A leading '+' that isn't a real E.164 number still gets rejected."""
    with pytest.raises(InvalidPhoneNumber, match="length"):
        normalize_phone("+1234567")  # only 7 digits


def test_normalize_phone_rejects_non_us_without_plus():
    """A 12-digit non-US number without '+' shouldn't be silently
    coerced into a US-style format. Force the user to include '+'."""
    with pytest.raises(InvalidPhoneNumber, match="E.164"):
        normalize_phone("442079460958")
