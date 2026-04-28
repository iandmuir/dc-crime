"""FastAPI application factory."""
import logging
import sys as _sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from wswdy.alerts import AdminAlerter
from wswdy.config import get_settings
from wswdy.db import connect, init_schema
from wswdy.notifiers.email import EmailNotifier
from wswdy.notifiers.whatsapp import WhatsAppMcpNotifier
from wswdy.routes import health, public

PKG_DIR = Path(__file__).resolve().parent
STATIC_DIR = PKG_DIR / "static"
TEMPLATES_DIR = PKG_DIR / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = FastAPI(title="wswdy", version="0.1.0")
    app.state.settings = settings

    app.state.db = connect(settings.db_path)
    init_schema(app.state.db)

    app.state.email_notifier = EmailNotifier(
        host=settings.smtp_host,
        port=settings.smtp_port,
        user=settings.smtp_user,
        password=settings.smtp_pass,
        sender=settings.smtp_from,
    )
    app.state.whatsapp_notifier = WhatsAppMcpNotifier(
        base_url=settings.whatsapp_mcp_url,
        token=settings.whatsapp_mcp_token,
    )
    app.state.alerter = AdminAlerter(
        db=app.state.db,
        email=app.state.email_notifier,
        admin_email=settings.admin_email,
        ha_webhook_url=settings.ha_webhook_url,
    )

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(health.router)
    app.include_router(public.router)

    return app


if "pytest" not in _sys.modules:
    app = create_app()
