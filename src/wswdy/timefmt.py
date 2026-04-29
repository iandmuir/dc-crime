"""Timestamp formatting helpers shared across templates and routes."""
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def to_eastern(utc_str: str | None, fmt: str = "%Y-%m-%d %H:%M %Z") -> str:
    """Convert a UTC ISO timestamp string to America/New_York for display.

    Accepts both tz-aware and tz-naive forms (SQLite's CURRENT_TIMESTAMP
    writes naive 'YYYY-MM-DD HH:MM:SS' UTC strings). Empty / None input
    returns "" so templates don't have to guard.
    """
    if not utc_str:
        return ""
    dt = datetime.fromisoformat(str(utc_str).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(ET).strftime(fmt)
