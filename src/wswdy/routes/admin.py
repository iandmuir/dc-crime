from datetime import date

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from wswdy.repos import subscribers as subs_repo
from wswdy.repos.fetch_log import last_attempt
from wswdy.repos.send_log import recent_failures, send_volume_last_n_days
from wswdy.repos.subscribers import list_by_status
from wswdy.tokens import sign

router = APIRouter()


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
    pending = list_by_status(db, "PENDING")
    # Generate per-subscriber review tokens so the admin can approve inline.
    pending_with_tokens = [
        {
            **dict(s),
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
        "approved": list_by_status(db, "APPROVED"),
        "rejected": list_by_status(db, "REJECTED"),
        "unsubscribed": list_by_status(db, "UNSUBSCRIBED"),
        "last_fetch": last_attempt(db),
        "send_volume": send_volume_last_n_days(db, n=7, today=str(date.today())),
        "failures": recent_failures(db, limit=20),
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
