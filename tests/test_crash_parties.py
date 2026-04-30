"""Tests for the crash parties feed: client mapping, repo upsert/group,
display humanization, and api integration."""
import httpx
import pytest
import respx

from wswdy.clients.dc_crash_details import (
    _feature_to_record,
    _yn,
    fetch_parties_for_crashes,
    humanize_plate_state,
    humanize_vehicle,
    party_is_interesting,
)
from wswdy.repos.crash_parties import (
    list_by_crimeids,
    prune_orphans,
    upsert_many,
)


def _attrs(**overrides):
    base = {
        "PERSONID": "P1", "CRIMEID": "C1", "CCN": "12345",
        "PERSONTYPE": "Driver", "AGE": 30,
        "FATAL": "N", "MAJORINJURY": "N", "MINORINJURY": "N",
        "VEHICLEID": "V1",
        "INVEHICLETYPE": "Passenger Car/Station Wagon/Jeep",
        "TICKETISSUED": "N", "LICENSEPLATESTATE": "DC",
        "IMPAIRED": "N", "SPEEDING": "N",
    }
    base.update(overrides)
    return {"attributes": base}


# ---------- Y/N helper ----------

def test_yn_handles_strings():
    assert _yn("Y") == 1
    assert _yn("y") == 1
    assert _yn("N") == 0
    assert _yn("") == 0
    assert _yn(None) == 0


# ---------- _feature_to_record ----------

def test_feature_to_record_minimal():
    rec = _feature_to_record(_attrs())
    assert rec["id"] == "P1"
    assert rec["crimeid"] == "C1"
    assert rec["person_type"] == "Driver"
    assert rec["age"] == 30
    assert rec["vehicle_type"] == "Passenger Car/Station Wagon/Jeep"
    assert rec["fatal"] == 0


def test_feature_to_record_translates_yn_flags():
    rec = _feature_to_record(_attrs(IMPAIRED="Y", SPEEDING="Y", FATAL="Y"))
    assert rec["impaired"] == 1
    assert rec["speeding"] == 1
    assert rec["fatal"] == 1


def test_feature_to_record_skips_when_no_id():
    bad = _attrs(PERSONID="")
    assert _feature_to_record(bad) is None


# ---------- humanizers ----------

@pytest.mark.parametrize("raw,expected", [
    ("Passenger Car/Station Wagon/Jeep", "Car"),
    ("PASSENGER CAR/AUTOMOBILE", "Car"),
    ("Sport Utility Vehicle", "SUV"),
    ("SUV (Sports Utility Vehicle)", "SUV"),
    ("Pickup Truck", "Pickup"),
    ("MOTOR CYCLE", "Motorcycle"),
    ("Bus", "Bus"),
    ("School Bus", "School bus"),
    ("Moped/Scooter", "Moped"),
])
def test_humanize_vehicle_known_categories(raw, expected):
    assert humanize_vehicle(raw) == expected


@pytest.mark.parametrize("raw", [
    None, "", "Unknown", "0", "Computer Hardware/ Software",
    "Drugs/ Narcotics", "Other",
])
def test_humanize_vehicle_drops_junk(raw):
    assert humanize_vehicle(raw) is None


def test_humanize_vehicle_passes_through_unknown_but_real_values():
    """A vehicle string we haven't mapped but isn't junk should still display
    (with whitespace stripped) so we don't drop legitimate edge cases."""
    assert humanize_vehicle("  Limo  ") == "Limo"


def test_humanize_plate_state_keeps_codes():
    assert humanize_plate_state("DC") == "DC"
    assert humanize_plate_state("VA") == "VA"
    assert humanize_plate_state(" MD ") == "MD"
    assert humanize_plate_state("USG") == "USG"


@pytest.mark.parametrize("raw", [None, "", "Unknown", "Uk", "0", "Ou", "None"])
def test_humanize_plate_state_drops_junk(raw):
    assert humanize_plate_state(raw) is None


# ---------- party_is_interesting ----------

def test_drivers_always_interesting():
    assert party_is_interesting({"person_type": "Driver"})


def test_pedestrians_and_cyclists_always_interesting():
    assert party_is_interesting({"person_type": "Pedestrian"})
    assert party_is_interesting({"person_type": "Bicyclist"})


def test_uninjured_passengers_dropped():
    assert not party_is_interesting({"person_type": "Passenger"})


def test_injured_passenger_kept():
    assert party_is_interesting({"person_type": "Passenger", "minor_injury": 1})


def test_speeding_other_party_kept():
    assert party_is_interesting({"person_type": "Other", "speeding": 1})


# ---------- fetch_parties_for_crashes ----------

@respx.mock
async def test_fetch_parties_batches_by_crimeid():
    """With 120 crimeids and batch_size=50 we expect 3 round-trips."""
    captured_urls = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_urls.append(str(request.url))
        return httpx.Response(200, json={"features": [_attrs()]})

    respx.get(host="maps2.dcgis.dc.gov").mock(side_effect=handler)
    crimeids = [f"C{i}" for i in range(120)]
    out = await fetch_parties_for_crashes(crimeids=crimeids, batch_size=50)
    assert len(captured_urls) == 3
    # One sample feature in each batch, deduped by id → just 1 record
    assert len(out) == 1


@respx.mock
async def test_fetch_parties_empty_crimeids_no_request():
    out = await fetch_parties_for_crashes(crimeids=[])
    assert out == []


# ---------- repos/crash_parties ----------

def _party(id="P1", crimeid="C1", **overrides):
    base = {
        "id": id, "crimeid": crimeid, "ccn": "x",
        "person_type": "Driver", "age": 30,
        "fatal": 0, "major_injury": 0, "minor_injury": 0,
        "vehicle_id": "V1", "vehicle_type": "Passenger Car/Station Wagon/Jeep",
        "license_state": "DC",
        "ticket_issued": 0, "impaired": 0, "speeding": 0,
    }
    base.update(overrides)
    return base


def test_upsert_then_update(db):
    a, u = upsert_many(db, [_party("P1"), _party("P2")])
    assert (a, u) == (2, 0)
    a, u = upsert_many(db, [_party("P1", impaired=1)])
    assert (a, u) == (0, 1)
    row = dict(db.execute("SELECT impaired FROM crash_parties WHERE id='P1'").fetchone())
    assert row["impaired"] == 1


def test_list_by_crimeids_groups_correctly(db):
    upsert_many(db, [
        _party("P1", crimeid="C1"),
        _party("P2", crimeid="C1"),
        _party("P3", crimeid="C2"),
    ])
    grouped = list_by_crimeids(db, ["C1", "C2", "C-no-such"])
    assert sorted(p["id"] for p in grouped["C1"]) == ["P1", "P2"]
    assert sorted(p["id"] for p in grouped["C2"]) == ["P3"]
    assert "C-no-such" not in grouped


def test_list_by_crimeids_empty_input_returns_empty_dict(db):
    assert list_by_crimeids(db, []) == {}


def test_prune_orphans_drops_parties_without_matching_crash(db):
    # Seed a crash so one party has a parent
    db.execute(
        "INSERT INTO crashes (id, lat, lon) VALUES ('C-keep', 38.9, -77.0)"
    )
    db.commit()
    upsert_many(db, [_party("P-keep", crimeid="C-keep"),
                     _party("P-orphan", crimeid="C-gone")])
    n = prune_orphans(db)
    assert n == 1
    remaining = [r["id"] for r in db.execute("SELECT id FROM crash_parties")]
    assert remaining == ["P-keep"]
