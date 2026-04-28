from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from wswdy.repos import subscribers as subs_repo
from wswdy.repos.crimes import list_in_radius_window
from wswdy.tiers import classify
from wswdy.tokens import TokenError, verify

router = APIRouter()

_WINDOWS = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}


@router.get("/api/crimes")
async def api_crimes(request: Request, subscriber: str, token: str, window: str = "24h"):
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
            "ccn": r["ccn"], "offense": r["offense"], "method": r["method"],
            "block": r["block_address"], "report_dt": r["report_dt"],
            "tier": classify(r["offense"], r["method"]),
        },
    } for r in rows]
    return JSONResponse({"type": "FeatureCollection", "features": features})
