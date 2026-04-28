"""FastAPI application factory."""
import logging
import sys as _sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from wswdy.config import get_settings
from wswdy.db import connect, init_schema
from wswdy.routes import health

PKG_DIR = Path(__file__).resolve().parent
STATIC_DIR = PKG_DIR / "static"


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = FastAPI(title="wswdy", version="0.1.0")
    app.state.settings = settings

    # DB connection — single shared connection, WAL mode tolerates concurrent readers
    app.state.db = connect(settings.db_path)
    init_schema(app.state.db)

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(health.router)

    return app


if "pytest" not in _sys.modules:
    app = create_app()
