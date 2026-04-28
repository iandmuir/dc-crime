"""Seed the local DB with synthetic subscribers + crimes for UI testing.

Usage: python scripts/seed.py
Reads WSWDY_DB_PATH from .env — make sure you're running against a dev DB.
"""
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Make src/ importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wswdy.config import get_settings
from wswdy.db import connect, init_schema
from wswdy.repos import subscribers as subs_repo
from wswdy.repos.crimes import upsert_many


SAMPLE_OFFENSES = [
    ("ROBBERY", "GUN", 1),
    ("ASSAULT W/DANGEROUS WEAPON", "GUN", 1),
    ("BURGLARY", None, 2),
    ("ARSON", None, 2),
    ("MOTOR VEHICLE THEFT", None, 3),
    ("THEFT F/AUTO", None, 4),
    ("THEFT/OTHER", None, 4),
]


def main():
    settings = get_settings()
    if "test" not in settings.db_path and "dev" not in settings.db_path \
       and not settings.db_path.startswith("./"):
        print(f"refuse to seed prod DB: {settings.db_path}")
        sys.exit(1)

    db = connect(settings.db_path)
    init_schema(db)

    # Subscribers
    for sid, name, ch, contact, lat, lon in [
        ("dev-jane", "Jane", "email", "jane@example.com", 38.9097, -77.0319),
        ("dev-bob",  "Bob",  "whatsapp", "+12025550100",   38.9050, -77.0420),
    ]:
        if subs_repo.get(db, sid):
            continue
        subs_repo.insert_pending(
            db, sid=sid, display_name=name,
            email=contact if ch == "email" else None,
            phone=contact if ch == "whatsapp" else None,
            preferred_channel=ch,
            address_text=f"{name}'s address, DC", lat=lat, lon=lon, radius_m=1000,
        )
        subs_repo.set_status(db, sid, "APPROVED")
    print("subscribers seeded")

    # Crimes — 30 random points within ~2km of Logan Circle, last 30 days
    now = datetime.now(timezone.utc)
    crimes = []
    for i in range(30):
        offense, method, _tier = random.choice(SAMPLE_OFFENSES)
        crimes.append({
            "ccn": f"DEV-{i:04d}",
            "offense": offense, "method": method, "shift": "DAY",
            "block_address": f"{1000 + i*10} block of 14th St NW",
            "lat": 38.9097 + random.uniform(-0.012, 0.012),
            "lon": -77.0319 + random.uniform(-0.012, 0.012),
            "report_dt": (now - timedelta(hours=random.randint(1, 24*30))).isoformat(timespec="seconds"),
            "start_dt": None, "end_dt": None,
            "ward": "2", "district": "THIRD", "raw_json": "{}",
        })
    added, updated = upsert_many(db, crimes)
    print(f"crimes seeded: +{added} ~{updated}")


if __name__ == "__main__":
    main()
