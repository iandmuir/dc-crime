"""Scan the WhatsApp bridge SQLite for inbound STOP messages and unsubscribe.

Background:
- The bridge writes every inbound message (is_from_me=0) into its own
  store/messages.db. The bridge also keeps whatsmeow's session state in
  store/whatsapp.db, which has a whatsmeow_lid_map table mapping
  WhatsApp's privacy "Linked IDs" back to phone numbers.
- For some users WhatsApp routes inbound replies through the phone-form
  chat ("12026422880@s.whatsapp.net"); for others (especially anyone who
  has migrated to LIDs) they arrive as "<lid>@lid". We handle both:
    * sender = bare phone digits → match against subscriber.phone directly
    * sender = LID → look up in whatsmeow_lid_map.lid -> pn, then match
- After resolution we strip non-digits from both sides and compare.

Cadence: scheduled every few minutes via APScheduler. Each run queries only
messages newer than the last seen timestamp (tracked in app_state) so we
don't reprocess history.

Match rule: trimmed, case-insensitive `STOP`. We deliberately don't accept
"STOP NOW" / "Please stop" / etc. — exact word only — to avoid false
positives from "stop and frisk" or other legitimate message content.
"""
import logging
import re
import sqlite3
from pathlib import Path

from wswdy.notifiers.whatsapp import WhatsAppMcpNotifier
from wswdy.repos import app_state, subscribers as subs_repo

log = logging.getLogger(__name__)

LAST_SEEN_KEY = "inbound_scanner_last_seen"
DEFAULT_BACKFILL_LIMIT = 200  # rows to look at on first run if no cursor stored
STOP_PATTERN = re.compile(r"^\s*stop\s*$", re.IGNORECASE)


def _normalize_phone(phone: str | None) -> str:
    """Strip everything except digits."""
    return "".join(c for c in (phone or "") if c.isdigit())


def _open_bridge_readonly(bridge_db_path: str) -> sqlite3.Connection:
    """Open the bridge's SQLite read-only so we can never accidentally write."""
    # uri=True lets us pass mode=ro
    uri = f"file:{bridge_db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _open_whatsmeow_readonly(messages_db_path: str) -> sqlite3.Connection | None:
    """Open whatsapp.db (whatsmeow's session/state DB, sibling to messages.db)
    read-only. Returns None if the file is missing — the scanner falls back
    to direct phone matching only."""
    wm_path = Path(messages_db_path).parent / "whatsapp.db"
    if not wm_path.exists():
        log.info("inbound_scan: whatsapp.db not found at %s; LID mapping disabled", wm_path)
        return None
    uri = f"file:{wm_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _resolve_sender_phone(wm_db: sqlite3.Connection | None, sender: str) -> str:
    """Resolve a bridge sender ID (LID or phone-form) to bare phone digits.

    Some inbound messages have sender = bare phone digits ("12026422880").
    Others (since WhatsApp's LID migration) have sender = a LID like
    "225687730401334". We check whatsmeow_lid_map and return the phone if
    a mapping exists; otherwise return sender unchanged so the caller's
    direct-phone match path can still try."""
    if wm_db is None:
        return sender
    row = wm_db.execute(
        "SELECT pn FROM whatsmeow_lid_map WHERE lid = ?", (sender,),
    ).fetchone()
    if row:
        return row["pn"]
    return sender


async def run_inbound_scan(
    *,
    db: sqlite3.Connection,
    bridge_db_path: str,
    whatsapp: WhatsAppMcpNotifier,
) -> dict:
    """One pass of the scanner. Returns a small status dict for logging."""
    bridge_path = Path(bridge_db_path)
    if not bridge_path.exists():
        log.warning("inbound_scan: bridge db not found at %s", bridge_db_path)
        return {"status": "bridge_missing"}

    last_seen = app_state.get(db, LAST_SEEN_KEY)
    bridge = _open_bridge_readonly(bridge_db_path)
    wm_db = _open_whatsmeow_readonly(bridge_db_path)

    try:
        if last_seen:
            rows = bridge.execute(
                """SELECT chat_jid, sender, content, timestamp
                     FROM messages
                    WHERE is_from_me = 0
                      AND timestamp > ?
                 ORDER BY timestamp ASC""",
                (last_seen,),
            ).fetchall()
        else:
            # First run: just look at recent inbound, don't try to match
            # the full history (could be years of group chat).
            rows = bridge.execute(
                """SELECT chat_jid, sender, content, timestamp
                     FROM messages
                    WHERE is_from_me = 0
                 ORDER BY timestamp DESC
                    LIMIT ?""",
                (DEFAULT_BACKFILL_LIMIT,),
            ).fetchall()
            rows = list(reversed(rows))  # process oldest -> newest

        if not rows:
            return {"status": "no_new_messages"}

        # Resolve every STOP sender's phone via the LID map BEFORE we close
        # the whatsmeow handle. (We close it together with `bridge` below.)
        resolved: list[tuple[sqlite3.Row, str]] = []
        for row in rows:
            if not STOP_PATTERN.match(row["content"] or ""):
                continue
            phone_digits = _normalize_phone(_resolve_sender_phone(wm_db, row["sender"]))
            resolved.append((row, phone_digits))
    finally:
        bridge.close()
        if wm_db is not None:
            wm_db.close()

    # Build a phone -> subscriber lookup once
    actives = subs_repo.list_active(db)
    phone_to_sub = {
        _normalize_phone(s["phone"]): s for s in actives if s.get("phone")
    }

    seen_count = len(rows)
    unsub_count = 0
    confirmations: list[tuple[str, str]] = []  # (recipient, name) for reply messages

    for row, phone_digits in resolved:
        sub = phone_to_sub.get(phone_digits)
        if not sub:
            log.info(
                "inbound_scan: STOP from sender=%s (resolved=%s) but no matching APPROVED subscriber",
                row["sender"], phone_digits,
            )
            continue
        log.info(
            "inbound_scan: unsubscribing %s (%s) on STOP from %s",
            sub["display_name"], sub["id"], row["sender"],
        )
        subs_repo.set_status(db, sub["id"], "UNSUBSCRIBED")
        unsub_count += 1
        confirmations.append((sub["phone"], sub["display_name"]))

    # Advance the cursor to the latest timestamp seen this batch
    last_ts = rows[-1]["timestamp"]
    app_state.set_value(db, LAST_SEEN_KEY, str(last_ts))

    # Send confirmation replies (best-effort; failures don't undo the unsub)
    for phone, name in confirmations:
        try:
            await whatsapp.send(
                recipient=phone, subject="",
                text=(
                    f"Hi, {name} — you've been unsubscribed from DC crime briefings. "
                    f"Sign up again any time at https://dccrime.iandmuir.com"
                ),
                image_path=None,
            )
        except Exception:
            log.exception("inbound_scan: confirmation reply to %s failed", phone)

    return {
        "status": "ok",
        "scanned": seen_count,
        "unsubscribed": unsub_count,
    }
