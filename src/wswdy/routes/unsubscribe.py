from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import TokenError, verify

router = APIRouter()


def _verify(request: Request, sid: str, token: str) -> bool:
    try:
        payload = verify(request.app.state.settings.hmac_secret, token,
                         purpose="unsubscribe")
        return payload["subscriber_id"] == sid
    except TokenError:
        return False


@router.get("/u/{sid}", response_class=HTMLResponse)
async def unsubscribe_get(request: Request, sid: str, token: str):
    if not _verify(request, sid, token):
        return Response(status_code=400, content="invalid token")
    sub = subs_repo.get(request.app.state.db, sid)
    if not sub:
        return Response(status_code=404)
    from wswdy.main import templates
    return templates.TemplateResponse(
        request, "unsubscribe.html",
        {"sub": sub, "token": token, "done": sub["status"] == "UNSUBSCRIBED"},
    )


@router.post("/u/{sid}", response_class=HTMLResponse)
async def unsubscribe_post(request: Request, sid: str, token: str):
    if not _verify(request, sid, token):
        return Response(status_code=400, content="invalid token")
    db = request.app.state.db
    if not subs_repo.get(db, sid):
        return Response(status_code=404)
    subs_repo.set_status(db, sid, "UNSUBSCRIBED")
    sub = subs_repo.get(db, sid)
    from wswdy.main import templates
    return templates.TemplateResponse(
        request, "unsubscribe.html", {"sub": sub, "token": token, "done": True},
    )
