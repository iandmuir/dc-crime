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

CREATE TABLE IF NOT EXISTS crashes (
  id              TEXT PRIMARY KEY,    -- DC's CRIMEID (per-crash identifier)
  ccn             TEXT,
  report_dt       TIMESTAMP,           -- maps to FROMDATE in the feed (when the crash happened)
  last_updated    TIMESTAMP,           -- maps to LASTUPDATEDATE in the feed
  address         TEXT,
  lat             REAL NOT NULL,
  lon             REAL NOT NULL,
  fatal           INTEGER NOT NULL DEFAULT 0,    -- total fatalities across all parties
  major_injury    INTEGER NOT NULL DEFAULT 0,    -- total people with major injuries
  minor_injury    INTEGER NOT NULL DEFAULT 0,    -- total people with minor injuries
  ped_fatal       INTEGER NOT NULL DEFAULT 0,
  ped_major       INTEGER NOT NULL DEFAULT 0,
  bike_fatal      INTEGER NOT NULL DEFAULT 0,
  bike_major      INTEGER NOT NULL DEFAULT 0,
  total_vehicles  INTEGER NOT NULL DEFAULT 0,
  total_pedestrians INTEGER NOT NULL DEFAULT 0,
  total_bicycles  INTEGER NOT NULL DEFAULT 0,
  speeding        INTEGER NOT NULL DEFAULT 0,
  impaired        INTEGER NOT NULL DEFAULT 0,    -- any party impaired
  ward            TEXT,
  raw_json        TEXT,
  fetched_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS crashes_report_dt_idx ON crashes(report_dt);
CREATE INDEX IF NOT EXISTS crashes_geo_idx ON crashes(lat, lon);

CREATE TABLE IF NOT EXISTS admin_alerts (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_type     TEXT NOT NULL,
  message        TEXT NOT NULL,
  created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  suppressed_until TIMESTAMP
);

-- Generic key/value store for app-level bookkeeping (e.g. last-checked
-- timestamps for the inbound STOP-message scanner).
CREATE TABLE IF NOT EXISTS app_state (
  key            TEXT PRIMARY KEY,
  value          TEXT,
  updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection in WAL mode with row dict access."""
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()
