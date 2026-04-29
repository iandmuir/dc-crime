from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from wswdy.repos.fetch_log import last_attempt
from wswdy.repos.send_log import recent_failures, send_volume_last_n_days
from wswdy.repos.subscribers import list_by_status
from wswdy.tokens import sign

router = APIRouter()


@router.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request, token: str = ""):
    settings = request.app.state.settings
    if not token or token != settings.admin_token:
        return Response(status_code=401, content="unauthorized")

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
