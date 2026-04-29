"""FastAPI application factory with scheduler lifespan."""
import logging
import sys as _sys
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from wswdy.alerts import AdminAlerter
from wswdy.config import get_settings
from wswdy.db import connect, init_schema
from wswdy.jobs.fetch import run_fetch
from wswdy.jobs.fetch_crashes import run_crash_fetch
from wswdy.jobs.health import run_health_snapshot
from wswdy.jobs.inbound_scanner import run_inbound_scan
from wswdy.jobs.prune import run_prune
from wswdy.jobs.send import run_send_if_ready
from wswdy.notifiers.email import EmailNotifier
from wswdy.notifiers.whatsapp import WhatsAppMcpNotifier
from wswdy.routes import (
    admin,
    admin_review,
    api_crashes,
    api_crimes,
    api_preview,
    health,
    map_view,
    public,
    unsubscribe,
)
from wswdy.scheduler import build_scheduler

from wswdy.timefmt import to_eastern

PKG_DIR = Path(__file__).resolve().parent
STATIC_DIR = PKG_DIR / "static"
TEMPLATES_DIR = PKG_DIR / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
# Templates use {{ value | et }} to render UTC timestamps as America/New_York.
templates.env.filters["et"] = to_eastern


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = app.state.settings

    async def fetch_job():
        await run_fetch(
            db=app.state.db, feed_url=str(settings.mpd_feed_url),
            alerter=app.state.alerter, fixture_path=settings.fixture_mpd_path,
        )

    async def send_job():
        """Hourly adaptive trigger. Fetches MPD data, then sends the daily
        digest only if yesterday's data has landed or we've hit the cutoff."""
        from wswdy.clients.geoapify import render_static_map

        # Step 1: refresh the feeds before deciding (cheap, ~1500 rows each).
        # Crashes are best-effort — failure shouldn't block the send.
        try:
            await run_fetch(
                db=app.state.db, feed_url=str(settings.mpd_feed_url),
                alerter=app.state.alerter, fixture_path=settings.fixture_mpd_path,
            )
        except Exception:
            logging.getLogger(__name__).exception(
                "send_job: pre-send MPD fetch failed; will evaluate freshness from existing data"
            )
        try:
            await run_crash_fetch(db=app.state.db)
        except Exception:
            logging.getLogger(__name__).exception(
                "send_job: pre-send crash fetch failed; digest will use whatever is in the DB"
            )

        # Step 2: decide whether to send
        async def render(*, center_lat, center_lon, radius_m, markers, out_path):
            return await render_static_map(
                api_key=settings.geoapify_api_key,
                center_lat=center_lat, center_lon=center_lon,
                radius_m=radius_m, markers=markers, out_path=out_path,
            )

        now_iso = datetime.now(UTC).isoformat(timespec="seconds")
        # Default static_map_dir to {log_dir}/static_maps for backwards compat;
        # in production set WSWDY_STATIC_MAP_DIR to a path the WhatsApp bridge
        # user can read (it loads media by absolute file path).
        static_map_dir = Path(
            settings.static_map_dir or f"{settings.log_dir}/static_maps"
        )
        result = await run_send_if_ready(
            db=app.state.db,
            email=app.state.email_notifier,
            whatsapp=app.state.whatsapp_notifier,
            alerter=app.state.alerter,
            base_url=settings.base_url,
            hmac_secret=settings.hmac_secret,
            now_iso=now_iso,
            cutoff_hour_et=settings.send_cutoff_hour_et,
            render_static_map=render,
            static_map_dir=static_map_dir,
        )
        logging.getLogger(__name__).info("send_job result: %s", result)

    async def prune_job():
        run_prune(app.state.db,
                  today_iso=datetime.now(UTC).isoformat(timespec="seconds"),
                  days=90)

    async def health_job():
        await run_health_snapshot(
            db=app.state.db, email=app.state.email_notifier,
            admin_email=settings.admin_email, today=str(date.today()),
        )

    async def inbound_job():
        if not settings.bridge_db_path:
            return  # scanner disabled
        try:
            result = await run_inbound_scan(
                db=app.state.db,
                bridge_db_path=settings.bridge_db_path,
                whatsapp=app.state.whatsapp_notifier,
            )
            if result.get("unsubscribed"):
                logging.getLogger(__name__).info(
                    "inbound scan: %s", result,
                )
        except Exception:
            logging.getLogger(__name__).exception("inbound scan failed")

    scheduler = build_scheduler(
        fetch_fn=fetch_job, send_fn=send_job,
        prune_fn=prune_job, health_fn=health_job,
        inbound_fn=inbound_job if settings.bridge_db_path else None,
    )
    scheduler.start()
    app.state.scheduler = scheduler
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        app.state.db.close()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = FastAPI(title="WTFDC", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings

    app.state.db = connect(settings.db_path)
    init_schema(app.state.db)

    app.state.email_notifier = EmailNotifier(
        host=settings.smtp_host, port=settings.smtp_port,
        user=settings.smtp_user, password=settings.smtp_pass,
        sender=settings.smtp_from,
    )
    app.state.whatsapp_notifier = WhatsAppMcpNotifier(
        base_url=settings.whatsapp_mcp_url, token=settings.whatsapp_mcp_token,
    )
    app.state.alerter = AdminAlerter(
        db=app.state.db, email=app.state.email_notifier,
        admin_email=settings.admin_email, ha_webhook_url=settings.ha_webhook_url,
    )

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(admin.router)
    app.include_router(admin_review.router)
    app.include_router(api_crashes.router)
    app.include_router(api_crimes.router)
    app.include_router(api_preview.router)
    app.include_router(health.router)
    app.include_router(map_view.router)
    app.include_router(public.router)
    app.include_router(unsubscribe.router)
    return app


if "pytest" not in _sys.modules:
    app = create_app()
