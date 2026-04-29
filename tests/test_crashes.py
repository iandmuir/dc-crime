"""Tests for crash data: client mapping, repo upsert/query, and tier classification."""
import json

import httpx
import pytest
import respx

from wswdy.clients.dc_crashes import _feature_to_record, fetch_recent_crashes
from wswdy.repos.crashes import list_in_radius_window, prune_older_than, upsert_many
from wswdy.tiers import classify_crash, crash_tier_label


# ---------- classify_crash ----------

def test_classify_crash_fatal_takes_priority():
    assert classify_crash({"fatal": 1, "major_injury": 1, "minor_injury": 1}) == 1


def test_classify_crash_major():
    assert classify_crash({"fatal": 0, "major_injury": 2, "minor_injury": 0}) == 2


def test_classify_crash_minor():
    assert classify_crash({"fatal": 0, "major_injury": 0, "minor_injury": 1}) == 3


def test_classify_crash_property_only():
    assert classify_crash({"fatal": 0, "major_injury": 0, "minor_injury": 0}) == 4


def test_crash_tier_label():
    assert crash_tier_label(1) == "fatal"
    assert crash_tier_label(4) == "property damage"


# ---------- _feature_to_record ----------

def _feature(**props):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-77.03, 38.91]},
        "properties": {
            "CRIMEID": "C1", "CCN": "C1-CCN",
            "FROMDATE": 1777147789000,  # 2026-04-25
            "LASTUPDATEDATE": 1777386735000,  # 2026-04-28
            "ADDRESS": "1500 14TH ST NW",
            "FATAL_PEDESTRIAN": 0, "FATAL_BICYCLIST": 0, "FATAL_DRIVER": 0,
            "FATALPASSENGER": 0, "FATALOTHER": 0,
            "MAJORINJURIES_PEDESTRIAN": 0, "MAJORINJURIES_BICYCLIST": 0,
            "MAJORINJURIES_DRIVER": 0, "MAJORINJURIESPASSENGER": 0,
            "MAJORINJURIESOTHER": 0,
            "MINORINJURIES_PEDESTRIAN": 0, "MINORINJURIES_BICYCLIST": 0,
            "MINORINJURIES_DRIVER": 0, "MINORINJURIESPASSENGER": 0,
            "MINORINJURIESOTHER": 0,
            "PEDESTRIANSIMPAIRED": 0, "BICYCLISTSIMPAIRED": 0, "DRIVERSIMPAIRED": 0,
            "SPEEDING_INVOLVED": 0,
            "TOTAL_VEHICLES": 2, "TOTAL_PEDESTRIANS": 0, "TOTAL_BICYCLES": 0,
            "WARD": "Ward 1",
            **props,
        },
    }


def test_feature_to_record_minimal():
    rec = _feature_to_record(_feature())
    assert rec is not None
    assert rec["id"] == "C1"
    assert rec["lat"] == 38.91 and rec["lon"] == -77.03
    assert rec["report_dt"].startswith("2026-04-25")
    assert rec["last_updated"].startswith("2026-04-28")
    assert rec["fatal"] == 0 and rec["major_injury"] == 0


def test_feature_to_record_aggregates_fatalities_across_roles():
    rec = _feature_to_record(_feature(FATAL_PEDESTRIAN=1, FATAL_DRIVER=1))
    assert rec["fatal"] == 2
    assert rec["ped_fatal"] == 1


def test_feature_to_record_pedestrian_major_injury_split_out():
    rec = _feature_to_record(_feature(MAJORINJURIES_PEDESTRIAN=1))
    assert rec["major_injury"] == 1
    assert rec["ped_major"] == 1


def test_feature_to_record_impaired_any_role():
    rec = _feature_to_record(_feature(BICYCLISTSIMPAIRED=1))
    assert rec["impaired"] == 1


def test_feature_to_record_handles_null_injury_fields():
    """DC's "*_OTHER" fields are sometimes null; we coalesce to 0 not crash."""
    rec = _feature_to_record(_feature(
        MAJORINJURIESOTHER=None, MINORINJURIESOTHER=None, FATALOTHER=None,
    ))
    assert rec is not None
    assert rec["fatal"] == 0


def test_feature_to_record_skips_records_missing_id_or_geometry():
    bad_id = _feature()
    bad_id["properties"]["CRIMEID"] = ""
    bad_id["properties"]["OBJECTID"] = None
    assert _feature_to_record(bad_id) is None

    bad_geom = _feature()
    bad_geom["geometry"] = {"type": "Point", "coordinates": []}
    assert _feature_to_record(bad_geom) is None

    no_date = _feature()
    no_date["properties"]["FROMDATE"] = None
    assert _feature_to_record(no_date) is None


# ---------- fetch_recent_crashes ----------

@respx.mock
async def test_fetch_recent_crashes_calls_with_window():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={"features": [_feature()]})

    respx.get(host="maps2.dcgis.dc.gov").mock(side_effect=handler)
    out = await fetch_recent_crashes(lookback_days=7)
    assert len(out) == 1
    assert "FROMDATE >= timestamp" in captured["params"]["where"]
    assert captured["params"]["f"] == "geojson"


@respx.mock
async def test_fetch_recent_crashes_filters_invalid_features():
    bad_features = [
        _feature(),  # ok
        {"type": "Feature", "geometry": {}, "properties": {}},  # invalid
        _feature(CRIMEID="C2"),  # ok
    ]
    respx.get(host="maps2.dcgis.dc.gov").mock(
        return_value=httpx.Response(200, json={"features": bad_features})
    )
    out = await fetch_recent_crashes()
    assert len(out) == 2
    assert {r["id"] for r in out} == {"C1", "C2"}


# ---------- repos/crashes ----------

def _crash(id="C1", lat=38.91, lon=-77.03, report_dt="2026-04-28T10:00:00+00:00",
           fatal=0, major_injury=0, minor_injury=0):
    return {
        "id": id, "ccn": "X", "report_dt": report_dt,
        "last_updated": "2026-04-29T00:00:00+00:00",
        "address": "test addr", "lat": lat, "lon": lon,
        "fatal": fatal, "major_injury": major_injury, "minor_injury": minor_injury,
        "ped_fatal": 0, "ped_major": 0, "bike_fatal": 0, "bike_major": 0,
        "total_vehicles": 1, "total_pedestrians": 0, "total_bicycles": 0,
        "speeding": 0, "impaired": 0, "ward": "Ward 1", "raw_json": "{}",
    }


def test_upsert_many_inserts_then_updates(db):
    # Schema is bootstrapped by the conftest db fixture.
    a, u = upsert_many(db, [_crash("A"), _crash("B")])
    assert (a, u) == (2, 0)

    # Re-upserting changes counts to "updated"
    a, u = upsert_many(db, [_crash("A", major_injury=1)])
    assert (a, u) == (0, 1)
    rows = list(db.execute("SELECT id, major_injury FROM crashes ORDER BY id"))
    assert dict(rows[0]) == {"id": "A", "major_injury": 1}


def test_list_in_radius_window_filters_by_geo_and_time(db):
    # Two crashes ~150m apart; one at center, one outside the requested window.
    upsert_many(db, [
        _crash("near_in_window", lat=38.9097, lon=-77.0319,
               report_dt="2026-04-28T10:00:00+00:00"),
        _crash("near_out_of_window", lat=38.9097, lon=-77.0319,
               report_dt="2026-04-20T10:00:00+00:00"),
        _crash("far", lat=39.0, lon=-77.0,
               report_dt="2026-04-28T10:00:00+00:00"),
    ])
    out = list_in_radius_window(
        db, lat=38.9097, lon=-77.0319, radius_m=500,
        start="2026-04-25T00:00:00+00:00",
        end="2026-04-29T00:00:00+00:00",
    )
    assert {r["id"] for r in out} == {"near_in_window"}


def test_prune_older_than_drops_old_rows(db):
    upsert_many(db, [
        _crash("old", report_dt="2025-01-01T00:00:00+00:00"),
        _crash("new", report_dt="2026-04-28T00:00:00+00:00"),
    ])
    n = prune_older_than(db, "2026-01-01T00:00:00+00:00")
    assert n == 1
    rows = list(db.execute("SELECT id FROM crashes"))
    assert [r["id"] for r in rows] == ["new"]
