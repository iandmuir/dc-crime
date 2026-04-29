import pytest

from wswdy.repos.subscribers import (
    get,
    insert_pending,
    list_active,
    list_by_status,
    set_last_sent,
    set_status,
)


def _new(db, **overrides):
    args = dict(
        sid="abc12345",
        display_name="Jane",
        email="jane@example.com",
        phone=None,
        preferred_channel="email",
        address_text="1500 14th St NW",
        lat=38.9097,
        lon=-77.0319,
        radius_m=1000,
    )
    args.update(overrides)
    return insert_pending(db, **args)


def test_insert_and_get(db):
    _new(db)
    s = get(db, "abc12345")
    assert s["display_name"] == "Jane"
    assert s["status"] == "PENDING"
    assert s["preferred_channel"] == "email"


def test_get_missing_returns_none(db):
    assert get(db, "nope") is None


def test_set_status_to_approved_stamps_approved_at(db):
    _new(db)
    set_status(db, "abc12345", "APPROVED")
    s = get(db, "abc12345")
    assert s["status"] == "APPROVED"
    assert s["approved_at"] is not None


def test_set_status_to_unsubscribed_stamps_unsubscribed_at(db):
    _new(db)
    set_status(db, "abc12345", "APPROVED")
    set_status(db, "abc12345", "UNSUBSCRIBED")
    s = get(db, "abc12345")
    assert s["unsubscribed_at"] is not None


def test_set_last_sent(db):
    _new(db)
    set_last_sent(db, "abc12345", "2026-04-28T10:00:00Z")
    s = get(db, "abc12345")
    assert s["last_sent_at"] == "2026-04-28T10:00:00Z"


def test_list_active_only_returns_approved(db):
    _new(db, sid="a")
    _new(db, sid="b")
    _new(db, sid="c")
    set_status(db, "a", "APPROVED")
    set_status(db, "b", "APPROVED")
    set_status(db, "b", "UNSUBSCRIBED")
    actives = list_active(db)
    assert [s["id"] for s in actives] == ["a"]


def test_invalid_status_raises(db):
    _new(db)
    with pytest.raises(ValueError):
        set_status(db, "abc12345", "WHATEVER")


def test_list_by_status_returns_correct_rows(db):
    _new(db, sid="a")
    _new(db, sid="b")
    _new(db, sid="c")
    set_status(db, "a", "APPROVED")
    rows = list_by_status(db, "APPROVED")
    assert len(rows) == 1 and rows[0]["id"] == "a"


def test_list_by_status_invalid_raises(db):
    with pytest.raises(ValueError):
        list_by_status(db, "BOGUS")
