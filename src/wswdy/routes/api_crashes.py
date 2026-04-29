"""GeoJSON crash endpoint for the map page.

Mirrors api_crimes.py — same auth (subscriber + map token), same window
choices (24h / 7d / 30d). Crash data has a 3-5 day publishing lag so the
24h window is usually empty; the map UI is responsible for not making
that look like a bug (e.g. by labeling the count appropriately)."""
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from wswdy.repos import subscribers as subs_repo
from wswdy.repos.crashes import list_in_radius_window
from wswdy.tiers import classify_crash
from wswdy.tokens import TokenError, verify

router = APIRouter()

_WINDOWS = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}


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
            "fatal": r["fatal"],
            "major_injury": r["major_injury"],
            "minor_injury": r["minor_injury"],
            # We track ped_/bike_ at fatal+major granularity; if either > 0
            # the crash involved a struck pedestrian/cyclist with notable harm.
            "ped_struck": ((r["ped_fatal"] or 0) + (r["ped_major"] or 0)) > 0,
            "bike_struck": ((r["bike_fatal"] or 0) + (r["bike_major"] or 0)) > 0,
            "vehicles": r["total_vehicles"],
        },
    } for r in rows]
    return JSONResponse({"type": "FeatureCollection", "features": features})
