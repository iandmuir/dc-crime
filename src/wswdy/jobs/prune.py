"""Prune crimes older than N days."""
import sqlite3
from datetime import datetime, timedelta

from wswdy.repos.crimes import prune_older_than


def run_prune(db: sqlite3.Connection, *, today_iso: str, days: int = 90) -> int:
    """Delete crimes with report_dt older than `days` days. Returns count deleted."""
    cutoff = datetime.fromisoformat(today_iso) - timedelta(days=days)
    return prune_older_than(db, cutoff.isoformat(timespec="seconds"))
