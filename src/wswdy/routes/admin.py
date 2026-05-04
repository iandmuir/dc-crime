from datetime import UTC, date, datetime, timedelta

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from wswdy.districts import _normalize_district, district_for
from wswdy.repos import subscribers as subs_repo
from wswdy.repos.fetch_log import last_attempt
from wswdy.repos.pdf_ingest_log import latest_per_district_kind
from wswdy.repos.send_log import recent_failures, send_volume_last_n_days
from wswdy.repos.subscribers import list_by_status
from wswdy.tokens import sign

router = APIRouter()

DISTRICTS = ("1D", "2D", "3D", "4D", "5D", "6D", "7D")
PDF_KINDS = ("crime", "arrest")


def _coverage_status(ingested_at: str | None) -> tuple[str, float | None]:
    """Map a last-ingest timestamp to (traffic-light status, hours-ago).

    Status is one of 'green' (<24h), 'yellow' (24-48h), 'red' (>48h or
    never). Hours-ago is None when we've never ingested for that cell."""
    if not ingested_at:
        return "red", None
    try:
        dt = datetime.fromisoformat(ingested_at.replace(" ", "T"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
    except ValueError:
        return "red", None
    hours = (datetime.now(UTC) - dt).total_seconds() / 3600
    if hours < 24:
        return "green", hours
    if hours < 48:
        return "yellow", hours
    return "red", hours


def _crimes_24h_by_district(db) -> dict[str, int]:
    """Count crimes ingested in the last 24h per district. Filters on
    ``fetched_at`` (when WE got the row) rather than ``report_dt`` (when the
    crime happened), because MPD's ArcGIS feed lags 24-72h — using report_dt
    would near-always return zero for the "today's fetch" view this drives.

    The ArcGIS feed stores district as a bare digit (``'1'``..``'7'``) while
    the rest of the app uses ``'1D'``..``'7D'`` — normalize before bucketing."""
    cutoff = (datetime.now(UTC) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    rows = db.execute(
        "SELECT district, COUNT(*) AS n FROM crimes "
        "WHERE fetched_at >= ? AND district IS NOT NULL "
        "GROUP BY district",
        (cutoff,),
    ).fetchall()
    counts: dict[str, int] = {}
    for r in rows:
        d = _normalize_district(r["district"])
        if d:
            counts[d] = counts.get(d, 0) + r["n"]
    return counts


def _crashes_24h_by_district(db) -> dict[str, int]:
    """Count crashes ingested in the last 24h per MPD district. Crashes don't
    carry a district column (the DC feed only gives ward), so we point-in-
    polygon each crash's lat/lon against the cached district boundaries.
    Filter is on ``fetched_at`` for the same lag reason as crimes — see
    ``_crimes_24h_by_district``."""
    cutoff = (datetime.now(UTC) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    rows = db.execute(
        "SELECT lat, lon FROM crashes WHERE fetched_at >= ?",
        (cutoff,),
    ).fetchall()
    counts: dict[str, int] = {}
    for r in rows:
        d = district_for(r["lat"], r["lon"])
        if d:
            counts[d] = counts.get(d, 0) + 1
    return counts


def _build_pdf_coverage(db) -> list[dict]:
    """One row per district × {crime, arrest}. Used by the admin coverage tracker.

    Each row also carries ``crimes_24h`` and ``crashes_24h`` — counts from the
    canonical ArcGIS feed (independent of the LISTSERV emails), so admins can
    spot-check that the email-reported numbers line up with what the public
    feed shows for that district."""
    latest = {(r["district"], r["kind"]): r for r in latest_per_district_kind(db)}
    crimes_today = _crimes_24h_by_district(db)
    crashes_today = _crashes_24h_by_district(db)
    out = []
    for district in DISTRICTS:
        cells = []
        for kind in PDF_KINDS:
            row = latest.get((district, kind))
            ingested = row["ingested_at"] if row else None
            status, hours = _coverage_status(ingested)
            cells.append({
                "kind": kind,
                "status": status,
                "hours": hours,
                "ingested_at": ingested,
                "records": row["records"] if row else 0,
                "source_file": row["source_file"] if row else None,
            })
        out.append({
            "district": district,
            "cells": cells,
            "crimes_24h": crimes_today.get(district, 0),
            "crashes_24h": crashes_today.get(district, 0),
        })
    return out


def _check_admin(request: Request, token: str) -> Response | None:
    if not token or token != request.app.state.settings.admin_token:
        return Response(status_code=401, content="unauthorized")
    return None


@router.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request, token: str = ""):
    if (resp := _check_admin(request, token)) is not None:
        return resp

    settings = request.app.state.settings
    db = request.app.state.db

    def _with_map_token(sub: dict) -> dict:
        """Add a signed map token so admins can preview each subscriber's
        view of their own neighborhood map."""
        return {
            **dict(sub),
            "map_token": sign(
                settings.hmac_secret,
                purpose="map",
                subscriber_id=sub["id"],
            ),
        }

    pending = list_by_status(db, "PENDING")
    # Pending subscribers get an additional review token for the inline
    # Approve / Reject buttons.
    pending_with_tokens = [
        {
            **_with_map_token(s),
            "review_token": sign(
                settings.hmac_secret,
                purpose="approve",
                subscriber_id=s["id"],
                ttl_seconds=7 * 86400,
            ),
        }
        for s in pending
    ]

    from wswdy.main import templates
    return templates.TemplateResponse(request, "admin.html", {
        "pending": pending_with_tokens,
        "approved": [_with_map_token(s) for s in list_by_status(db, "APPROVED")],
        "rejected": [_with_map_token(s) for s in list_by_status(db, "REJECTED")],
        "unsubscribed": [_with_map_token(s) for s in list_by_status(db, "UNSUBSCRIBED")],
        "last_fetch": last_attempt(db),
        "send_volume": send_volume_last_n_days(db, n=7, today=str(date.today())),
        "failures": recent_failures(db, limit=20),
        "pdf_coverage": _build_pdf_coverage(db),
        "token": token,
    })


@router.post("/admin/subscriber/{sid}/delete")
async def admin_delete_subscriber(request: Request, sid: str, token: str = Form(...)):
    if (resp := _check_admin(request, token)) is not None:
        return resp
    subs_repo.delete(request.app.state.db, sid)
    return RedirectResponse(url=f"/admin?token={token}", status_code=303)


@router.post("/admin/subscriber/{sid}/unsubscribe")
async def admin_unsubscribe_subscriber(request: Request, sid: str, token: str = Form(...)):
    if (resp := _check_admin(request, token)) is not None:
        return resp
    db = request.app.state.db
    if subs_repo.get(db, sid):
        subs_repo.set_status(db, sid, "UNSUBSCRIBED")
    return RedirectResponse(url=f"/admin?token={token}", status_code=303)
