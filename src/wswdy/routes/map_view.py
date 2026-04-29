from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import TokenError, verify

router = APIRouter()


@router.get("/map/{sid}", response_class=HTMLResponse)
async def map_view(request: Request, sid: str, token: str):
    settings = request.app.state.settings
    try:
        payload = verify(settings.hmac_secret, token, purpose="map")
    except TokenError as e:
        return Response(status_code=400, content=f"invalid token: {e}")
    if payload["subscriber_id"] != sid:
        return Response(status_code=400, content="token mismatch")

    sub = subs_repo.get(request.app.state.db, sid)
    if not sub:
        return Response(status_code=404)

    from wswdy.main import templates
    return templates.TemplateResponse(
        request, "map.html",
        {"sub": sub, "token": token, "maptiler_key": settings.maptiler_api_key},
    )
