"""crash_parties table — upsert + grouped-by-crashid queries."""
import sqlite3

_COLUMNS = (
    "id", "crimeid", "ccn", "person_type", "age",
    "fatal", "major_injury", "minor_injury",
    "vehicle_id", "vehicle_type", "license_state",
    "ticket_issued", "impaired", "speeding",
)


def upsert_many(db: sqlite3.Connection, parties: list[dict]) -> tuple[int, int]:
    """Returns (n_added, n_updated). Match key is `id` (PERSONID)."""
    added = updated = 0
    cols = ", ".join(_COLUMNS)
    placeholders = ", ".join(["?"] * len(_COLUMNS))
    update_set = ", ".join(f"{c}=?" for c in _COLUMNS if c != "id")

    for p in parties:
        cur = db.execute("SELECT 1 FROM crash_parties WHERE id=?", (p["id"],)).fetchone()
        if cur:
            db.execute(
                f"UPDATE crash_parties SET {update_set} WHERE id=?",
                (*[p.get(col) for col in _COLUMNS if col != "id"], p["id"]),
            )
            updated += 1
        else:
            db.execute(
                f"INSERT INTO crash_parties ({cols}) VALUES ({placeholders})",
                tuple(p.get(col) for col in _COLUMNS),
            )
            added += 1
    db.commit()
    return added, updated


def list_by_crimeids(db: sqlite3.Connection, crimeids: list[str]) -> dict[str, list[dict]]:
    """Return parties grouped by crimeid: {crimeid: [party, party, ...]}.
    Returns {} for an empty crimeid list. Empty crashes (no party rows) are
    not included as keys — caller should default to []."""
    if not crimeids:
        return {}
    placeholders = ",".join(["?"] * len(crimeids))
    rows = db.execute(
        f"SELECT * FROM crash_parties WHERE crimeid IN ({placeholders})",
        tuple(crimeids),
    ).fetchall()
    grouped: dict[str, list[dict]] = {}
    for r in rows:
        d = dict(r)
        grouped.setdefault(d["crimeid"], []).append(d)
    return grouped


def prune_orphans(db: sqlite3.Connection) -> int:
    """Delete crash_parties rows whose crimeid is no longer in the crashes
    table. Called from the prune job to keep parties pruned alongside crashes."""
    cur = db.execute(
        "DELETE FROM crash_parties WHERE crimeid NOT IN (SELECT id FROM crashes)"
    )
    db.commit()
    return cur.rowcount
