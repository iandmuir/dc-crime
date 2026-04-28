from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from wswdy.geo import in_dc_bbox
from wswdy.ratelimit import RateLimiter
from wswdy.repos.crimes import list_in_radius_window
from wswdy.tiers import classify

router = APIRouter()
_rl = RateLimiter(max_requests=30, window_s=60)


class PreviewBody(BaseModel):
    lat: float
    lon: float
    radius_m: int = Field(ge=200, le=2000)


@router.post("/api/preview")
async def api_preview(request: Request, body: PreviewBody):
    ip = request.client.host if request.client else "unknown"
    if not _rl.check(ip):
        return Response(status_code=429, content="rate limited")
    if not in_dc_bbox(body.lat, body.lon):
        return Response(status_code=400, content="coordinates outside DC")

    now = datetime.now(UTC)
    start = (now - timedelta(days=7)).isoformat(timespec="seconds")
    end = now.isoformat(timespec="seconds")
    rows = list_in_radius_window(
        request.app.state.db, body.lat, body.lon, body.radius_m,
        start=start, end=end,
    )
    counts = {1: 0, 2: 0, 3: 0, 4: 0}
    for row in rows:
        counts[classify(row["offense"], row["method"])] += 1
    total = len(rows)
    return JSONResponse({
        "window_days": 7,
        "total": total,
        "avg_per_day": total / 7,
        "by_tier": {str(k): v for k, v in counts.items()},
    })
