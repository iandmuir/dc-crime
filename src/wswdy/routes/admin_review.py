from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from wswdy.notifiers.base import dispatch
from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import TokenError, sign, verify

router = APIRouter()


def _verify_or_400(request: Request, token: str) -> str | Response:
    secret = request.app.state.settings.hmac_secret
    try:
        payload = verify(secret, token, purpose="approve")
        return payload["subscriber_id"]
    except TokenError as e:
        return Response(status_code=400, content=f"invalid token: {e}")


@router.get("/a/{token}", response_class=HTMLResponse)
async def review_landing(request: Request, token: str):
    sid_or_resp = _verify_or_400(request, token)
    if isinstance(sid_or_resp, Response):
        return sid_or_resp
    sub = subs_repo.get(request.app.state.db, sid_or_resp)
    if not sub:
        return Response(status_code=404, content="subscriber not found")
    from wswdy.main import templates
    return templates.TemplateResponse(
        request, "admin_review.html", {"sub": sub, "token": token},
    )


@router.post("/a/{token}/approve")
async def review_approve(request: Request, token: str):
    sid_or_resp = _verify_or_400(request, token)
    if isinstance(sid_or_resp, Response):
        return sid_or_resp
    db = request.app.state.db
    sub = subs_repo.get(db, sid_or_resp)
    if not sub:
        return Response(status_code=404)

    subs_repo.set_status(db, sub["id"], "APPROVED")
    sub = subs_repo.get(db, sub["id"])

    settings = request.app.state.settings
    unsub_token = sign(settings.hmac_secret, purpose="unsubscribe",
                       subscriber_id=sub["id"])
    text = (
        f"Hi {sub['display_name']} — you're confirmed. ✓\n\n"
        f"You'll get your first DC crime briefing tomorrow morning at 6am, "
        f"covering the area within {sub['radius_m']:,}m of your home.\n\n"
        f"Unsubscribe anytime: {settings.base_url}/u/{sub['id']}?token={unsub_token}"
    )
    await dispatch(
        sub,
        email_notifier=request.app.state.email_notifier,
        whatsapp_notifier=request.app.state.whatsapp_notifier,
        subject=f"Welcome to wswdy, {sub['display_name']}",
        text=text, image_path=None,
    )

    return RedirectResponse(url=f"/a/{token}?done=approved", status_code=303)


@router.post("/a/{token}/reject")
async def review_reject(request: Request, token: str):
    sid_or_resp = _verify_or_400(request, token)
    if isinstance(sid_or_resp, Response):
        return sid_or_resp
    db = request.app.state.db
    if not subs_repo.get(db, sid_or_resp):
        return Response(status_code=404)
    subs_repo.set_status(db, sid_or_resp, "REJECTED")
    return RedirectResponse(url=f"/a/{token}?done=rejected", status_code=303)
