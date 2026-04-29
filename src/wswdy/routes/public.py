"""Signup form + POST /signup + JSON helpers used by the form."""
import logging

from fastapi import APIRouter, BackgroundTasks, Form, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from wswdy.clients.maptiler import GeocodeError, geocode_address
from wswdy.geo import in_dc_bbox
from wswdy.ids import new_subscriber_id
from wswdy.ratelimit import RateLimiter
from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import sign

logger = logging.getLogger(__name__)

router = APIRouter()
_signup_rl = RateLimiter(max_requests=10, window_s=3600)
_geocode_rl = RateLimiter(max_requests=60, window_s=60)


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


async def _notify_admin_of_signup(
    *, email_notifier, admin_email: str, subject: str, body: str,
) -> None:
    """Send the admin notification, swallowing any errors so the user-facing
    redirect already happened by the time SMTP is attempted."""
    try:
        await email_notifier.send(
            recipient=admin_email, subject=subject, text=body, image_path=None,
        )
    except Exception:
        logger.exception("admin signup notification failed")


@router.get("/", response_class=HTMLResponse)
async def signup_form(request: Request):
    from wswdy.main import templates

    return templates.TemplateResponse(request, "signup.html", {"error": None})


@router.get("/signup/thanks", response_class=HTMLResponse)
async def signup_thanks(request: Request, ch: str = ""):
    """Confirmation page after signup. `ch` carries the channel choice so we can
    show channel-specific guidance (e.g. a wa.me link to enable WhatsApp delivery)."""
    from wswdy.main import templates

    settings = request.app.state.settings
    # WhatsApp wa.me links want digits only — no "+" or punctuation.
    bridge_digits = "".join(c for c in settings.whatsapp_from_number if c.isdigit())
    return templates.TemplateResponse(request, "signup_thanks.html", {
        "channel": ch,
        "wa_bridge_digits": bridge_digits,
    })


@router.post("/signup")
async def signup_submit(
    request: Request,
    background_tasks: BackgroundTasks,
    display_name: str = Form(...),
    address_text: str = Form(...),
    preferred_channel: str = Form(...),
    radius_m: int = Form(...),
    email: str = Form(""),
    phone: str = Form(""),
    lat: float | None = Form(None),
    lon: float | None = Form(None),
):
    if not _signup_rl.check(_client_ip(request)):
        return Response(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            content="Too many signup attempts. Try again in an hour.",
        )

    settings = request.app.state.settings
    from wswdy.main import templates

    try:
        if lat is not None and lon is not None:
            if not in_dc_bbox(lat, lon):
                raise GeocodeError("address is outside DC")
            place = {"lat": lat, "lon": lon, "display": address_text}
        else:
            place = await geocode_address(address_text, api_key=settings.maptiler_api_key)
    except GeocodeError as e:
        return templates.TemplateResponse(
            request,
            "signup.html",
            {"error": str(e)},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    if preferred_channel not in {"email", "whatsapp"}:
        return Response(status_code=400, content="invalid channel")
    if preferred_channel == "email" and not email:
        return Response(status_code=400, content="email required")
    if preferred_channel == "whatsapp" and not phone:
        return Response(status_code=400, content="phone required")
    if not (200 <= radius_m <= 2000):
        return Response(status_code=400, content="radius out of range")

    sid = new_subscriber_id()
    subs_repo.insert_pending(
        request.app.state.db,
        sid=sid,
        display_name=display_name,
        email=email or None,
        phone=phone or None,
        preferred_channel=preferred_channel,
        address_text=address_text,
        lat=place["lat"],
        lon=place["lon"],
        radius_m=radius_m,
    )

    # Build the admin notification but send it in the background so the
    # user gets redirected to the thanks page immediately.
    token = sign(
        settings.hmac_secret,
        purpose="approve",
        subscriber_id=sid,
        ttl_seconds=7 * 86400,
    )
    review_url = f"{settings.base_url}/a/{token}"
    body = (
        f"New WTFDC signup from {display_name}.\n\n"
        f"Channel: {preferred_channel} ({email or phone})\n"
        f"Address: {place['display']}\n"
        f"Coords:  {place['lat']:.4f}, {place['lon']:.4f}\n"
        f"Radius:  {radius_m}m\n\n"
        f"Approve or reject:\n{review_url}\n"
    )
    background_tasks.add_task(
        _notify_admin_of_signup,
        email_notifier=request.app.state.email_notifier,
        admin_email=settings.admin_email,
        subject=f"[WTFDC] new signup: {display_name}",
        body=body,
    )

    return RedirectResponse(url=f"/signup/thanks?ch={preferred_channel}", status_code=303)


@router.get("/api/geocode", response_class=JSONResponse)
async def geocode_endpoint(request: Request, q: str):
    if not _geocode_rl.check(_client_ip(request)):
        return JSONResponse({"results": []}, status_code=429)
    settings = request.app.state.settings
    try:
        place = await geocode_address(q, api_key=settings.maptiler_api_key)
        return {
            "results": [
                {"lat": place["lat"], "lon": place["lon"], "display": place["display"]}
            ]
        }
    except GeocodeError:
        return {"results": []}
