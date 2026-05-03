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

-- One row per party (driver / passenger / pedestrian / cyclist) involved in
-- a crash. Multiple rows can share a crimeid (the crash) and a vehicle_id
-- (driver + their passengers). Sourced from layer 25 of the same DC public
-- safety feed; the parties layer has 800k+ historical rows but we only
-- ingest those tied to crashes already in our 30-day crashes window.
CREATE TABLE IF NOT EXISTS crash_parties (
  id              TEXT PRIMARY KEY,         -- PERSONID
  crimeid         TEXT NOT NULL,            -- joins to crashes.id
  ccn             TEXT,
  person_type     TEXT,                     -- 'Driver','Passenger','Pedestrian','Bicyclist','Other'
  age             INTEGER NOT NULL DEFAULT 0,  -- 0 = unknown
  fatal           INTEGER NOT NULL DEFAULT 0,
  major_injury    INTEGER NOT NULL DEFAULT 0,
  minor_injury    INTEGER NOT NULL DEFAULT 0,
  vehicle_id      TEXT,
  vehicle_type    TEXT,                     -- raw string from feed; humanized at display time
  license_state   TEXT,                     -- 'DC','VA','MD','USG','Diplomatic',...
  ticket_issued   INTEGER NOT NULL DEFAULT 0,
  impaired        INTEGER NOT NULL DEFAULT 0,
  speeding        INTEGER NOT NULL DEFAULT 0,
  fetched_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS crash_parties_crimeid_idx ON crash_parties(crimeid);

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

-- Per-CCN extras parsed out of MPD's daily LISTSERV crime PDFs. The PDFs
-- arrive ahead of the public ArcGIS feed and carry a useful LOCATION
-- field (Restaurant, Residence/Home, Church Synagogue Temple Mosque, etc)
-- that the API doesn't expose. Joined to crimes by ccn at read time —
-- the canonical crime row still comes from the API.
CREATE TABLE IF NOT EXISTS crime_extras (
  ccn            TEXT PRIMARY KEY,
  district       TEXT,                     -- '1D' .. '7D'
  psa            TEXT,
  location       TEXT,                     -- the new useful field
  source         TEXT NOT NULL DEFAULT 'pdf_listserv',
  ingested_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS crime_extras_district_idx ON crime_extras(district);

-- Arrests parsed from MPD's daily LISTSERV arrest PDFs. Standalone dataset
-- (no join to crimes — DC doesn't expose the link). Address is geocoded
-- once at upsert time via MapTiler and lat/lon stored alongside.
CREATE TABLE IF NOT EXISTS arrests (
  arrest_number    TEXT PRIMARY KEY,
  district         TEXT,                   -- '1D' .. '7D'
  psa              TEXT,
  arrest_dt        TIMESTAMP,              -- ISO UTC
  arrest_location  TEXT,                   -- raw multi-line address, normalized
  lat              REAL,                   -- geocoded; null if geocode failed
  lon              REAL,
  offender_first   TEXT,
  offender_last    TEXT,
  gender           TEXT,
  age              INTEGER,
  offense          TEXT,
  felony_misd      TEXT,                   -- 'FELONY' / 'MISDEMEANOR' / null
  officer          TEXT,
  ingested_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS arrests_arrest_dt_idx ON arrests(arrest_dt);
CREATE INDEX IF NOT EXISTS arrests_geo_idx ON arrests(lat, lon);
CREATE INDEX IF NOT EXISTS arrests_district_idx ON arrests(district);

-- One row per (district, kind, source-date) we successfully ingested. Drives
-- the admin coverage tracker — most-recent-per-(district,kind) row tells us
-- whether and when each district's report has landed today.
CREATE TABLE IF NOT EXISTS pdf_ingest_log (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  district       TEXT NOT NULL,            -- '1D' .. '7D'
  kind           TEXT NOT NULL,            -- 'crime' or 'arrest'
  source_file    TEXT,                     -- original PDF filename, for debugging
  records        INTEGER NOT NULL DEFAULT 0,
  ingested_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS pdf_ingest_log_lookup_idx
  ON pdf_ingest_log(district, kind, ingested_at);
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
