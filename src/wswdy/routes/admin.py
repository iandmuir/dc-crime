from datetime import date

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from wswdy.repos.fetch_log import last_attempt
from wswdy.repos.send_log import recent_failures, send_volume_last_n_days
from wswdy.repos.subscribers import list_by_status

router = APIRouter()


@router.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request, token: str = ""):
    expected = request.app.state.settings.admin_token
    if not token or token != expected:
        return Response(status_code=401, content="unauthorized")

    db = request.app.state.db
    from wswdy.main import templates
    return templates.TemplateResponse(request, "admin.html", {
        "pending": list_by_status(db, "PENDING"),
        "approved": list_by_status(db, "APPROVED"),
        "rejected": list_by_status(db, "REJECTED"),
        "unsubscribed": list_by_status(db, "UNSUBSCRIBED"),
        "last_fetch": last_attempt(db),
        "send_volume": send_volume_last_n_days(db, n=7, today=str(date.today())),
        "failures": recent_failures(db, limit=20),
        "token": token,
    })
