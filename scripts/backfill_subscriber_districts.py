#!/usr/bin/env python3
"""Tag every existing subscriber with their MPD police district.

After upgrading to per-subscriber district readiness, run this once to
populate ``subscribers.district`` for accounts that signed up before
the column existed. New signups get auto-tagged at insert time, so this
script is a one-shot migration aid.

Usage::

    /root/wswdy/.venv/bin/python scripts/backfill_subscriber_districts.py

Prereq: ``src/wswdy/static/dc_police_districts.geojson`` must exist.
Run ``scripts/fetch_district_boundaries.py`` first if it doesn't.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wswdy.config import get_settings  # noqa: E402
from wswdy.db import connect, init_schema  # noqa: E402
from wswdy.districts import district_for, reset_cache  # noqa: E402


def main() -> int:
    reset_cache()
    settings = get_settings()
    db = connect(settings.db_path)
    init_schema(db)

    rows = db.execute(
        "SELECT id, display_name, lat, lon, district FROM subscribers"
    ).fetchall()
    if not rows:
        print("no subscribers to backfill")
        return 0

    updated = unchanged = unmapped = 0
    for r in rows:
        existing = r["district"]
        district = district_for(r["lat"], r["lon"])
        if district is None:
            print(f"  ! {r['id']} ({r['display_name']!r}): "
                  f"point ({r['lat']:.4f}, {r['lon']:.4f}) outside every "
                  f"district polygon", file=sys.stderr)
            unmapped += 1
            continue
        if existing == district:
            unchanged += 1
            continue
        db.execute(
            "UPDATE subscribers SET district=? WHERE id=?", (district, r["id"]),
        )
        print(f"  {r['id']} ({r['display_name']!r}): "
              f"{existing or '∅'} → {district}")
        updated += 1
    db.commit()

    print(
        f"\nbackfill complete — updated={updated} unchanged={unchanged} "
        f"unmapped={unmapped} total={len(rows)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
