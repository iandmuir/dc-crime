"""GeoJSON crash endpoint for the map page.

Mirrors api_crimes.py — same auth (subscriber + map token), same window
choices (24h / 7d / 30d). Crash data has a 3-5 day publishing lag so the
24h window is usually empty; the map UI is responsible for not making
that look like a bug (e.g. by labeling the count appropriately).

Each feature exposes a structured `involved` and `injuries` payload built
from the original raw_json (DC's ArcGIS feed has per-role counts that we
don't normalize into columns — too many of them — but the popup wants
the full breakdown). See _expand_props below for the mapping.
"""
import json
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from wswdy.repos import subscribers as subs_repo
from wswdy.repos.crashes import list_in_radius_window
from wswdy.tiers import classify_crash
from wswdy.tokens import TokenError, verify

router = APIRouter()

_WINDOWS = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}


def _i(props: dict, key: str) -> int:
    """Coalesce a possibly-null integer field to 0."""
    v = props.get(key)
    return int(v) if v is not None else 0


def _expand_props(row: dict) -> dict:
    """Pull the per-role injury and involvement breakdown out of raw_json.

    The crashes table stores aggregate flags (fatal/major/minor totals plus
    ped/bike fatal+major splits) but the popup wants more — including
    minor injuries per role, taxi/government vehicle counts, etc. — so we
    parse raw_json on the read path. Each row is small and we cap features
    at radius+window so the parse cost is negligible."""
    try:
        p = json.loads(row.get("raw_json") or "{}")
    except (TypeError, ValueError):
        p = {}

    # Vehicles. TOTAL_VEHICLES includes taxis and government vehicles, so
    # subtract them to get a "private cars" count.
    total_v = _i(p, "TOTAL_VEHICLES")
    taxis = _i(p, "TOTAL_TAXIS")
    govs = _i(p, "TOTAL_GOVERNMENT")
    cars = max(0, total_v - taxis - govs)

    involved = {
        "cars": cars,
        "taxis": taxis,
        "government": govs,
        "bicycles": _i(p, "TOTAL_BICYCLES"),
        "pedestrians": _i(p, "TOTAL_PEDESTRIANS"),
    }

    injuries = {
        "pedestrian": {
            "fatal": _i(p, "FATAL_PEDESTRIAN"),
            "major": _i(p, "MAJORINJURIES_PEDESTRIAN"),
            "minor": _i(p, "MINORINJURIES_PEDESTRIAN"),
        },
        "bicyclist": {
            "fatal": _i(p, "FATAL_BICYCLIST"),
            "major": _i(p, "MAJORINJURIES_BICYCLIST"),
            "minor": _i(p, "MINORINJURIES_BICYCLIST"),
        },
        "driver": {
            "fatal": _i(p, "FATAL_DRIVER"),
            "major": _i(p, "MAJORINJURIES_DRIVER"),
            "minor": _i(p, "MINORINJURIES_DRIVER"),
        },
        # Passenger fields use no underscore (DC's schema quirk).
        "passenger": {
            "fatal": _i(p, "FATALPASSENGER"),
            "major": _i(p, "MAJORINJURIESPASSENGER"),
            "minor": _i(p, "MINORINJURIESPASSENGER"),
        },
    }

    factors = {
        "speeding": _i(p, "SPEEDING_INVOLVED") > 0,
        "impaired": (
            _i(p, "DRIVERSIMPAIRED")
            + _i(p, "PEDESTRIANSIMPAIRED")
            + _i(p, "BICYCLISTSIMPAIRED")
        ) > 0,
    }
    return {"involved": involved, "injuries": injuries, "factors": factors}


@router.get("/api/crashes")
async def api_crashes(request: Request, subscriber: str, token: str, window: str = "7d"):
    secret = request.app.state.settings.hmac_secret
    try:
        payload = verify(secret, token, purpose="map")
    except TokenError as e:
        return Response(status_code=401, content=f"invalid token: {e}")
    if payload["subscriber_id"] != subscriber:
        return Response(status_code=401, content="token mismatch")
    if window not in _WINDOWS:
        return Response(status_code=400, content="unknown window")

    sub = subs_repo.get(request.app.state.db, subscriber)
    if not sub:
        return Response(status_code=404)

    now = datetime.now(UTC)
    start = (now - _WINDOWS[window]).isoformat(timespec="seconds")
    end = now.isoformat(timespec="seconds")

    rows = list_in_radius_window(
        request.app.state.db, sub["lat"], sub["lon"], sub["radius_m"],
        start=start, end=end,
    )
    features = [{
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
        "properties": {
            "id": r["id"],
            "address": r["address"],
            "report_dt": r["report_dt"],
            "tier": classify_crash(r),
            # Aggregate counts (kept for backward compat / quick checks)
            "fatal": r["fatal"],
            "major_injury": r["major_injury"],
            "minor_injury": r["minor_injury"],
            "ped_struck": ((r["ped_fatal"] or 0) + (r["ped_major"] or 0)) > 0,
            "bike_struck": ((r["bike_fatal"] or 0) + (r["bike_major"] or 0)) > 0,
            "vehicles": r["total_vehicles"],
            # Rich breakdown (parsed from raw_json) for the popup
            **_expand_props(r),
        },
    } for r in rows]
    return JSONResponse({"type": "FeatureCollection", "features": features})
