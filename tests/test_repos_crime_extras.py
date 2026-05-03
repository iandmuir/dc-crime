"""Tests for repos.crime_extras."""
from wswdy.repos.crime_extras import get_by_ccn, get_many_by_ccn, upsert_many


def _row(ccn: str, location: str = "Restaurant", district: str = "1D") -> dict:
    return {"ccn": ccn, "district": district, "psa": "101", "location": location}


def test_upsert_inserts_new(db):
    added, updated = upsert_many(db, [_row("A"), _row("B")])
    assert (added, updated) == (2, 0)


def test_upsert_updates_existing(db):
    upsert_many(db, [_row("A", location="Restaurant")])
    added, updated = upsert_many(db, [_row("A", location="Residence/Home")])
    assert (added, updated) == (0, 1)
    assert get_by_ccn(db, "A")["location"] == "Residence/Home"


def test_get_by_ccn_returns_none_when_missing(db):
    assert get_by_ccn(db, "missing") is None


def test_get_many_by_ccn_only_returns_present_rows(db):
    upsert_many(db, [_row("A"), _row("B"), _row("C")])
    out = get_many_by_ccn(db, ["A", "C", "missing"])
    assert set(out.keys()) == {"A", "C"}
    assert out["A"]["location"] == "Restaurant"


def test_get_many_by_ccn_handles_empty_input(db):
    upsert_many(db, [_row("A")])
    assert get_many_by_ccn(db, []) == {}


def test_default_source_is_pdf_listserv(db):
    upsert_many(db, [_row("A")])
    assert get_by_ccn(db, "A")["source"] == "pdf_listserv"
