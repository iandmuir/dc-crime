"""SQLite connection helpers and schema bootstrap."""
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS subscribers (
  id              TEXT PRIMARY KEY,
  display_name    TEXT NOT NULL,
  email           TEXT,
  phone           TEXT,
  preferred_channel TEXT NOT NULL CHECK(preferred_channel IN ('email','whatsapp')),
  address_text    TEXT NOT NULL,
  lat             REAL NOT NULL,
  lon             REAL NOT NULL,
  radius_m        INTEGER NOT NULL,
  status          TEXT NOT NULL DEFAULT 'PENDING',
  created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  approved_at     TIMESTAMP,
  unsubscribed_at TIMESTAMP,
  last_sent_at    TIMESTAMP
);
CREATE INDEX IF NOT EXISTS subscribers_status_idx ON subscribers(status);

CREATE TABLE IF NOT EXISTS crimes (
  ccn            TEXT PRIMARY KEY,
  offense        TEXT NOT NULL,
  method         TEXT,
  shift          TEXT,
  block_address  TEXT,
  lat            REAL NOT NULL,
  lon            REAL NOT NULL,
  report_dt      TIMESTAMP NOT NULL,
  start_dt       TIMESTAMP,
  end_dt         TIMESTAMP,
  ward           TEXT,
  district       TEXT,
  raw_json       TEXT,
  fetched_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS crimes_report_dt_idx ON crimes(report_dt);
CREATE INDEX IF NOT EXISTS crimes_geo_idx ON crimes(lat, lon);

CREATE TABLE IF NOT EXISTS send_log (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  subscriber_id  TEXT NOT NULL REFERENCES subscribers(id),
  send_date      DATE NOT NULL,
  channel        TEXT NOT NULL,
  status         TEXT NOT NULL,
  error          TEXT,
  sent_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(subscriber_id, send_date, channel)
);

CREATE TABLE IF NOT EXISTS fetch_log (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  fetched_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status         TEXT NOT NULL,
  crimes_added   INTEGER,
  crimes_updated INTEGER,
  error          TEXT
);

CREATE TABLE IF NOT EXISTS admin_alerts (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_type     TEXT NOT NULL,
  message        TEXT NOT NULL,
  created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  suppressed_until TIMESTAMP
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection in WAL mode with row dict access."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True) if "/" in db_path or "\\" in db_path else None
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
