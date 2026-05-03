"""GeoJSON arrests endpoint for the map page.

Mirrors api_crimes.py / api_crashes.py — same auth (subscriber + map token)
and the same window choices (24h / 7d / 30d). Arrest data comes from MPD's
daily LISTSERV PDFs (parsed by wswdy.pdf_reports → wswdy.jobs.pdf_ingest)
rather than an API, so coverage depends on which districts the admin has
ingested today. Arrests with no successful geocode are excluded by the
underlying repo (lat IS NULL filter).

Each feature carries the offender's name, age, gender, offense, and
felony/misdemeanor classification. The classification drives the tier (1
felony, 2 misdemeanor, 3 unspecified) which the map UI uses for marker
color.
"""
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from wswdy.address import humanize_address
from wswdy.repos import subscribers as subs_repo
from wswdy.repos.arrests import list_in_radius_window
from wswdy.tiers import classify_arrest
from wswdy.tokens import TokenError, verify

router = APIRouter()

_WINDOWS = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}


def _full_name(first: str | None, last: str | None) -> str | None:
    """Combine first/last in title-case. Returns None if both are missing."""
    parts = [p.strip().title() for p in (first, last) if p and p.strip()]
    return " ".join(parts) if parts else None


@router.get("/api/arrests")
async def api_arrests(request: Request, subscriber: str, token: str, window: str = "7d"):
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

    features = []
    for r in rows:
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
            "properties": {
                "arrest_number": r["arrest_number"],
                "arrest_dt": r["arrest_dt"],
                "address": humanize_address(r.get("arrest_location")),
                "name": _full_name(r.get("offender_first"), r.get("offender_last")),
                "age": r.get("age"),
                "gender": r.get("gender"),
                "offense": r.get("offense"),
                "felony_misd": r.get("felony_misd"),
                "officer": r.get("officer"),
                "district": r.get("district"),
                "tier": classify_arrest(r),
            },
        })
    return JSONResponse({"type": "FeatureCollection", "features": features})
