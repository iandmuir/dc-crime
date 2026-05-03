from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from wswdy.geo import in_dc_bbox
from wswdy.ratelimit import RateLimiter
from wswdy.repos.arrests import list_in_radius_window as list_arrests_in_radius_window
from wswdy.repos.crashes import list_in_radius_window as list_crashes_in_radius_window
from wswdy.repos.crimes import list_in_radius_window as list_crimes_in_radius_window

router = APIRouter()
_rl = RateLimiter(max_requests=30, window_s=60)

# Preview window mirrors the live map default — 7 days. That's long
# enough to give a neighborhood-scale signal without showing pages of
# data, and matches the crash section's window in the daily digest.
_WINDOW_DAYS = 7


class PreviewBody(BaseModel):
    lat: float
    lon: float
    radius_m: int = Field(ge=200, le=2000)


@router.post("/api/preview")
async def api_preview(request: Request, body: PreviewBody):
    """Three-up activity preview shown on the signup page when a
    prospective subscriber types an address + adjusts the radius.

    Returns ``crimes`` / ``crashes`` / ``arrests`` totals (and
    per-day averages) for the last 7 days. The preview is meant to set
    expectations for the daily digest's volume — it's the
    one-look "is anything actually happening near me?" answer.
    """
    ip = request.client.host if request.client else "unknown"
    if not _rl.check(ip):
        return Response(status_code=429, content="rate limited")
    if not in_dc_bbox(body.lat, body.lon):
        return Response(status_code=400, content="coordinates outside DC")

    now = datetime.now(UTC)
    start = (now - timedelta(days=_WINDOW_DAYS)).isoformat(timespec="seconds")
    end = now.isoformat(timespec="seconds")

    crimes = list_crimes_in_radius_window(
        request.app.state.db, body.lat, body.lon, body.radius_m,
        start=start, end=end,
    )
    crashes = list_crashes_in_radius_window(
        request.app.state.db, body.lat, body.lon, body.radius_m,
        start=start, end=end,
    )
    # Arrests repo automatically excludes ungeocoded rows (lat IS NULL).
    arrests = list_arrests_in_radius_window(
        request.app.state.db, body.lat, body.lon, body.radius_m,
        start=start, end=end,
    )

    n_crimes = len(crimes)
    n_crashes = len(crashes)
    n_arrests = len(arrests)
    return JSONResponse({
        "window_days": _WINDOW_DAYS,
        "crimes": {
            "total": n_crimes,
            "avg_per_day": n_crimes / _WINDOW_DAYS,
        },
        "crashes": {
            "total": n_crashes,
            "avg_per_day": n_crashes / _WINDOW_DAYS,
        },
        "arrests": {
            "total": n_arrests,
            "avg_per_day": n_arrests / _WINDOW_DAYS,
        },
    })
