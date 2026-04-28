# DC Crime Alerts (wswdy) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the wswdy daily DC crime briefing service from scratch — Python FastAPI app, SQLite, daily scheduled MPD ingest, email + WhatsApp delivery, interactive map, manual-approval signup — running in a single LXC behind Cloudflare Tunnel at `dccrime.iandmuir.com`.

**Architecture:** Single Python 3.12 process (FastAPI + APScheduler + Uvicorn) backed by one SQLite file. Synchronous DB layer (sqlite3 module, WAL mode) with async HTTP/SMTP clients. Notifier protocol abstracts email vs WhatsApp delivery. HMAC-signed tokens for all public links — no token table. Frontend is server-rendered Jinja2 templates ported from the approved mockups.

**Tech Stack:** Python 3.12 · FastAPI · Uvicorn · APScheduler (AsyncIOScheduler) · SQLite (WAL) · Jinja2 · pydantic-settings · httpx · aiosmtplib · pytest · pytest-asyncio · Leaflet · MapTiler

**Spec:** `docs/superpowers/specs/2026-04-28-dc-crime-alerts-design.md`
**Mockups:** `mockups/{index,unsubscribe,map}.html` + `mockups/shared.css`

---

## File structure

```
dc-crime/
├── pyproject.toml                # project metadata + tool config (ruff, pytest)
├── requirements.txt              # pinned runtime deps
├── requirements-dev.txt          # dev/test deps
├── Makefile                      # dev / test / seed / fmt / lint
├── .env.example
├── .gitignore                    # already exists
├── README.md                     # already exists
├── docs/
│   ├── superpowers/{specs,plans}/...
│   ├── deploy.md                 # LXC + tunnel setup runbook
│   └── operations.md             # failure runbook
├── mockups/                      # kept as reference (already exists)
├── src/wswdy/
│   ├── __init__.py
│   ├── config.py                 # Settings via pydantic-settings
│   ├── main.py                   # FastAPI entrypoint + lifespan
│   ├── db.py                     # sqlite3 connection, schema init
│   ├── ids.py                    # short random subscriber IDs
│   ├── tokens.py                 # HMAC sign/verify
│   ├── tiers.py                  # classify(offense, method) -> tier
│   ├── geo.py                    # haversine, DC bbox check
│   ├── ratelimit.py              # in-memory IP rate limiter
│   ├── repos/
│   │   ├── __init__.py
│   │   ├── subscribers.py
│   │   ├── crimes.py
│   │   ├── send_log.py
│   │   ├── fetch_log.py
│   │   └── admin_alerts.py
│   ├── clients/
│   │   ├── __init__.py
│   │   ├── mpd.py                # MPD GeoJSON fetcher
│   │   ├── maptiler.py           # geocode + static map
│   │   └── whatsapp_mcp.py       # WhatsApp MCP HTTP client
│   ├── notifiers/
│   │   ├── __init__.py
│   │   ├── base.py               # Notifier protocol, SendResult, dispatch
│   │   ├── email.py              # EmailNotifier
│   │   ├── whatsapp.py           # WhatsAppMcpNotifier
│   │   └── fake.py               # FakeNotifier for tests
│   ├── alerts.py                 # AdminAlerter (email + HA + suppression)
│   ├── digest.py                 # build digest text + closest selection
│   ├── scheduler.py              # APScheduler setup
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── fetch.py
│   │   ├── send.py
│   │   ├── prune.py
│   │   └── health.py
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── public.py             # GET /, POST /signup
│   │   ├── admin_review.py       # /a/{token}
│   │   ├── unsubscribe.py        # /u/{id}
│   │   ├── map_view.py           # /map/{id}
│   │   ├── api_crimes.py         # /api/crimes
│   │   ├── api_preview.py        # /api/preview
│   │   ├── admin.py              # /admin
│   │   └── health.py             # /healthz
│   ├── templates/
│   │   ├── base.html
│   │   ├── signup.html
│   │   ├── unsubscribe.html
│   │   ├── map.html
│   │   ├── admin_review.html
│   │   ├── admin.html
│   │   ├── email/{admin_review,welcome,digest}.html
│   │   └── whatsapp/{welcome,digest}.txt
│   └── static/
│       └── shared.css
├── tests/
│   ├── conftest.py
│   ├── fixtures/mpd_sample.geojson
│   ├── test_tiers.py
│   ├── test_geo.py
│   ├── test_tokens.py
│   ├── test_ids.py
│   ├── test_db.py
│   ├── test_repos_*.py           # one per repo
│   ├── test_mpd.py
│   ├── test_maptiler.py
│   ├── test_whatsapp_mcp.py
│   ├── test_notifiers.py
│   ├── test_alerts.py
│   ├── test_digest.py
│   ├── test_ratelimit.py
│   ├── test_routes_*.py          # one per route module
│   ├── test_jobs_fetch.py
│   ├── test_jobs_send.py
│   └── test_e2e_smoke.py
├── deploy/
│   ├── dccrime.service
│   ├── cloudflared-config.yml.example
│   └── logrotate.conf
└── scripts/
    ├── seed.py
    └── backup.sh
```

**Boundary principles:** repos own SQL and return plain dicts/dataclasses; clients own external HTTP; notifiers own outbound delivery; routes own only request parsing + template rendering + dispatching to repos and notifiers; jobs orchestrate. No SQL outside `repos/`. No HTTP calls outside `clients/`. Tests against a real in-memory SQLite (no DB mocking) and FakeNotifier for delivery.

---

## Phase 1 — Foundations (Tasks 1–7)

### Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`, `requirements.txt`, `requirements-dev.txt`, `Makefile`, `src/wswdy/__init__.py`, `tests/__init__.py`, `tests/conftest.py`, `.env.example`

- [ ] **Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "wswdy"
version = "0.1.0"
description = "DC Crime Alerts daily briefing"
requires-python = ">=3.12"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
addopts = "-q"

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]
```

- [ ] **Step 2: Create requirements.txt**

```text
fastapi==0.115.0
uvicorn[standard]==0.32.0
jinja2==3.1.4
pydantic==2.9.2
pydantic-settings==2.5.2
apscheduler==3.10.4
httpx==0.27.2
aiosmtplib==3.0.2
python-multipart==0.0.12
itsdangerous==2.2.0
```

- [ ] **Step 3: Create requirements-dev.txt**

```text
-r requirements.txt
pytest==8.3.3
pytest-asyncio==0.24.0
pytest-mock==3.14.0
ruff==0.6.9
respx==0.21.1
```

- [ ] **Step 4: Create Makefile**

```makefile
.PHONY: install dev test fmt lint seed run-prod

install:
	pip install -r requirements-dev.txt
	pip install -e .

dev:
	WSWDY_ENV=dev uvicorn wswdy.main:app --reload --port 8000

run-prod:
	uvicorn wswdy.main:app --host 0.0.0.0 --port 8000 --workers 1

test:
	pytest

fmt:
	ruff format src tests

lint:
	ruff check src tests

seed:
	python scripts/seed.py
```

- [ ] **Step 5: Create empty package files**

```bash
mkdir -p src/wswdy tests
touch src/wswdy/__init__.py tests/__init__.py
```

- [ ] **Step 6: Create an empty tests/conftest.py (the `db` fixture is added in Task 3)**

```python
"""pytest fixtures shared across the suite."""
```

- [ ] **Step 7: Create .env.example**

```bash
# Server
WSWDY_ENV=dev
WSWDY_BASE_URL=http://localhost:8000
WSWDY_LOG_DIR=./logs
WSWDY_DB_PATH=./dccrime.db

# Secrets
HMAC_SECRET=replace-with-32+-random-bytes-base64
ADMIN_TOKEN=replace-with-static-token-for-/admin

# MapTiler
MAPTILER_API_KEY=

# MPD feed
MPD_FEED_URL=https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/FeatureServer/39/query?outFields=*&where=1%3D1&f=geojson

# Email (SMTP)
SMTP_HOST=
SMTP_PORT=587
SMTP_USER=
SMTP_PASS=
SMTP_FROM=wswdy <noreply@iandmuir.com>
ADMIN_EMAIL=iandmuir@gmail.com

# WhatsApp MCP
WHATSAPP_MCP_URL=
WHATSAPP_MCP_TOKEN=
WHATSAPP_FROM_NUMBER=+12024682709

# Home Assistant webhook (admin alerts)
HA_WEBHOOK_URL=

# Local dev only — when set, fetcher reads from this fixture instead of MPD_FEED_URL
WSWDY_FIXTURE_MPD_PATH=
```

- [ ] **Step 8: Install deps and verify scaffolding**

```bash
python -m venv .venv
. .venv/Scripts/activate    # or .venv/bin/activate on Linux
make install
pytest --version
```

Expected: `pytest 8.3.3` printed without errors.

- [ ] **Step 9: Commit**

```bash
git add pyproject.toml requirements.txt requirements-dev.txt Makefile src tests .env.example
git commit -m "chore: project scaffolding (pyproject, deps, makefile, package skeleton)"
```

---

### Task 2: Configuration loader (`src/wswdy/config.py`)

**Files:**
- Create: `src/wswdy/config.py`, `tests/test_config.py`

- [ ] **Step 1: Write the failing test**

`tests/test_config.py`:
```python
from wswdy.config import Settings


def test_settings_load_from_env(monkeypatch):
    monkeypatch.setenv("HMAC_SECRET", "abc")
    monkeypatch.setenv("ADMIN_TOKEN", "xyz")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", "/tmp/x.db")
    s = Settings()
    assert s.hmac_secret == "abc"
    assert s.admin_token == "xyz"
    assert s.maptiler_api_key == "k"
    assert s.db_path == "/tmp/x.db"
    assert s.smtp_port == 587  # default
    assert str(s.mpd_feed_url).startswith("https://")


def test_settings_missing_required_raises(monkeypatch):
    monkeypatch.delenv("HMAC_SECRET", raising=False)
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        Settings()
```

- [ ] **Step 2: Run — expect failure**

```bash
pytest tests/test_config.py -v
```
Expected: `ImportError: No module named 'wswdy.config'`.

- [ ] **Step 3: Implement Settings**

`src/wswdy/config.py`:
```python
from functools import lru_cache
from pydantic import HttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = Field("dev", alias="WSWDY_ENV")
    base_url: str = Field("http://localhost:8000", alias="WSWDY_BASE_URL")
    log_dir: str = Field("./logs", alias="WSWDY_LOG_DIR")
    db_path: str = Field("./dccrime.db", alias="WSWDY_DB_PATH")

    hmac_secret: str
    admin_token: str

    maptiler_api_key: str

    mpd_feed_url: HttpUrl = Field(
        "https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/FeatureServer/39/query"
        "?outFields=*&where=1%3D1&f=geojson",
        alias="MPD_FEED_URL",
    )
    fixture_mpd_path: str | None = Field(None, alias="WSWDY_FIXTURE_MPD_PATH")

    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_pass: str = ""
    smtp_from: str = "wswdy <noreply@iandmuir.com>"
    admin_email: str = "iandmuir@gmail.com"

    whatsapp_mcp_url: str = ""
    whatsapp_mcp_token: str = ""
    whatsapp_from_number: str = "+12024682709"

    ha_webhook_url: str = ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 4: Run — expect pass**

```bash
pytest tests/test_config.py -v
```
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/wswdy/config.py tests/test_config.py
git commit -m "feat(config): pydantic-settings loader with .env support"
```

---

### Task 3: Database connection + schema init (`src/wswdy/db.py`)

**Files:**
- Create: `src/wswdy/db.py`, `tests/test_db.py`

- [ ] **Step 1: Write the failing test**

`tests/test_db.py`:
```python
import sqlite3
from wswdy.db import connect, init_schema


def test_init_schema_creates_all_tables(tmp_path):
    db = tmp_path / "t.db"
    conn = connect(str(db))
    init_schema(conn)
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    names = {r["name"] for r in rows}
    assert {
        "subscribers", "crimes", "send_log", "fetch_log", "admin_alerts"
    } <= names


def test_init_schema_is_idempotent(tmp_path):
    db = tmp_path / "t.db"
    conn = connect(str(db))
    init_schema(conn)
    init_schema(conn)  # should not raise
    conn.execute("INSERT INTO subscribers (id, display_name, preferred_channel, "
                 "address_text, lat, lon, radius_m) "
                 "VALUES ('a', 'A', 'email', '1 St', 38.9, -77.0, 1000)")
    conn.commit()


def test_connect_uses_wal_mode(tmp_path):
    db = tmp_path / "t.db"
    conn = connect(str(db))
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"


def test_connect_rows_are_dicts(tmp_path):
    db = tmp_path / "t.db"
    conn = connect(str(db))
    init_schema(conn)
    conn.execute("INSERT INTO subscribers (id, display_name, preferred_channel, "
                 "address_text, lat, lon, radius_m) VALUES "
                 "('a', 'A', 'email', '1', 1.0, 2.0, 500)")
    conn.commit()
    row = conn.execute("SELECT * FROM subscribers WHERE id='a'").fetchone()
    assert row["display_name"] == "A"
```

- [ ] **Step 2: Run — expect failure**

```bash
pytest tests/test_db.py -v
```
Expected: ImportError on `wswdy.db`.

- [ ] **Step 3: Implement db.py**

`src/wswdy/db.py`:
```python
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
```

- [ ] **Step 4: Run — expect pass**

```bash
pytest tests/test_db.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Add db fixture to conftest.py**

`tests/conftest.py`:
```python
"""pytest fixtures shared across the suite."""
import pytest
from wswdy.db import connect, init_schema


@pytest.fixture
def db(tmp_path):
    """Per-test SQLite connection in a temp file (avoids shared-memory pitfalls)."""
    conn = connect(str(tmp_path / "test.db"))
    init_schema(conn)
    yield conn
    conn.close()
```

- [ ] **Step 6: Commit**

```bash
git add src/wswdy/db.py tests/test_db.py tests/conftest.py
git commit -m "feat(db): SQLite connection + schema init"
```

---

### Task 4: Severity tier classifier (`src/wswdy/tiers.py`)

**Files:**
- Create: `src/wswdy/tiers.py`, `tests/test_tiers.py`

- [ ] **Step 1: Write the parametrized test**

`tests/test_tiers.py`:
```python
import pytest
from wswdy.tiers import classify, tier_label

CASES = [
    # offense, method, expected tier
    ("HOMICIDE",                     None,    1),
    ("HOMICIDE",                     "GUN",   1),
    ("SEX ABUSE",                    None,    1),
    ("ASSAULT W/DANGEROUS WEAPON",   "GUN",   1),
    ("ASSAULT W/DANGEROUS WEAPON",   "OTHERS",1),
    ("ROBBERY",                      "GUN",   1),  # armed → tier 1
    ("ROBBERY",                      "KNIFE", 1),
    ("ROBBERY",                      "OTHERS",2),  # unarmed → tier 2
    ("ROBBERY",                      None,    2),
    ("BURGLARY",                     None,    2),
    ("ARSON",                        None,    2),
    ("MOTOR VEHICLE THEFT",          None,    3),
    ("THEFT F/AUTO",                 None,    4),
    ("THEFT/OTHER",                  None,    4),
]


@pytest.mark.parametrize("offense,method,expected", CASES)
def test_classify(offense, method, expected):
    assert classify(offense, method) == expected


def test_classify_unknown_offense_defaults_to_4():
    assert classify("UNKNOWN OFFENSE", None) == 4


def test_classify_is_case_insensitive():
    assert classify("homicide", None) == 1
    assert classify("Robbery", "gun") == 1


def test_tier_labels():
    assert tier_label(1) == "violent"
    assert tier_label(2) == "serious property"
    assert tier_label(3) == "vehicle"
    assert tier_label(4) == "petty"
```

- [ ] **Step 2: Run — expect failure**

```bash
pytest tests/test_tiers.py -v
```
Expected: ImportError.

- [ ] **Step 3: Implement classifier**

`src/wswdy/tiers.py`:
```python
"""Severity tier classifier.

Tier 1 (violent):       Homicide, Sex Abuse, Assault w/ Weapon, Armed Robbery
Tier 2 (serious prop):  Robbery (unarmed), Burglary, Arson
Tier 3 (vehicle):       Motor Vehicle Theft
Tier 4 (petty):         Theft from Auto, Theft/Other (default)
"""
from typing import Final

_TIER1: Final = {"HOMICIDE", "SEX ABUSE", "ASSAULT W/DANGEROUS WEAPON"}
_TIER2_PROPERTY: Final = {"BURGLARY", "ARSON"}
_TIER3: Final = {"MOTOR VEHICLE THEFT"}
_TIER4: Final = {"THEFT F/AUTO", "THEFT/OTHER"}
_ARMED_METHODS: Final = {"GUN", "KNIFE"}

_LABELS: Final = {1: "violent", 2: "serious property", 3: "vehicle", 4: "petty"}


def classify(offense: str, method: str | None) -> int:
    o = (offense or "").strip().upper()
    m = (method or "").strip().upper()

    if o in _TIER1:
        return 1
    if o == "ROBBERY":
        return 1 if m in _ARMED_METHODS else 2
    if o in _TIER2_PROPERTY:
        return 2
    if o in _TIER3:
        return 3
    if o in _TIER4:
        return 4
    return 4  # unknown offenses default to least-severe tier


def tier_label(tier: int) -> str:
    return _LABELS[tier]
```

- [ ] **Step 4: Run — expect pass**

```bash
pytest tests/test_tiers.py -v
```
Expected: all parametrized cases pass.

- [ ] **Step 5: Commit**

```bash
git add src/wswdy/tiers.py tests/test_tiers.py
git commit -m "feat(tiers): severity classifier with weapon-modifier rule"
```

---

### Task 5: HMAC token utility (`src/wswdy/tokens.py`)

**Files:**
- Create: `src/wswdy/tokens.py`, `tests/test_tokens.py`

- [ ] **Step 1: Write the failing test**

`tests/test_tokens.py`:
```python
import time
import pytest
from wswdy.tokens import sign, verify, TokenError

SECRET = "test-secret-32-bytes-long-base64ish"


def test_roundtrip_no_expiry():
    t = sign(SECRET, purpose="unsubscribe", subscriber_id="abc123")
    payload = verify(SECRET, t, purpose="unsubscribe")
    assert payload["subscriber_id"] == "abc123"


def test_roundtrip_with_expiry():
    t = sign(SECRET, purpose="approve", subscriber_id="abc", ttl_seconds=60)
    payload = verify(SECRET, t, purpose="approve")
    assert payload["subscriber_id"] == "abc"


def test_expired_token_rejected():
    t = sign(SECRET, purpose="approve", subscriber_id="abc", ttl_seconds=-1)
    with pytest.raises(TokenError, match="expired"):
        verify(SECRET, t, purpose="approve")


def test_wrong_purpose_rejected():
    t = sign(SECRET, purpose="approve", subscriber_id="abc")
    with pytest.raises(TokenError, match="purpose"):
        verify(SECRET, t, purpose="unsubscribe")


def test_tampered_token_rejected():
    t = sign(SECRET, purpose="map", subscriber_id="abc")
    head, sig = t.split(".")
    tampered = head + "X" + "." + sig
    with pytest.raises(TokenError):
        verify(SECRET, tampered, purpose="map")


def test_wrong_secret_rejected():
    t = sign(SECRET, purpose="map", subscriber_id="abc")
    with pytest.raises(TokenError):
        verify("different-secret", t, purpose="map")


def test_garbage_token_rejected():
    with pytest.raises(TokenError):
        verify(SECRET, "not.even.close", purpose="map")
```

- [ ] **Step 2: Run — expect failure**

```bash
pytest tests/test_tokens.py -v
```
Expected: ImportError.

- [ ] **Step 3: Implement tokens.py**

`src/wswdy/tokens.py`:
```python
"""HMAC-signed token utility.

Format: base64url(json_payload).base64url(hmac_sha256)
Payload: {"p": purpose, "s": subscriber_id, "e": expires_at_unix or null}
"""
import base64
import hmac
import hashlib
import json
import time
from typing import Any


class TokenError(Exception):
    """Raised when a token is malformed, tampered, expired, or wrong-purpose."""


def _b64encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def _b64decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def sign(secret: str, *, purpose: str, subscriber_id: str,
         ttl_seconds: int | None = None) -> str:
    expires = int(time.time()) + ttl_seconds if ttl_seconds is not None else None
    payload = {"p": purpose, "s": subscriber_id, "e": expires}
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    head = _b64encode(raw)
    sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
    return f"{head}.{_b64encode(sig)}"


def verify(secret: str, token: str, *, purpose: str) -> dict[str, Any]:
    try:
        head, sig_b64 = token.split(".", 1)
        raw = _b64decode(head)
        expected_sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
        actual_sig = _b64decode(sig_b64)
    except (ValueError, base64.binascii.Error) as e:
        raise TokenError(f"malformed token: {e}") from e

    if not hmac.compare_digest(expected_sig, actual_sig):
        raise TokenError("invalid signature")

    payload = json.loads(raw)
    if payload.get("p") != purpose:
        raise TokenError(f"wrong purpose: expected {purpose}, got {payload.get('p')}")

    expires = payload.get("e")
    if expires is not None and int(time.time()) > expires:
        raise TokenError("expired")

    return {"subscriber_id": payload["s"], "purpose": payload["p"]}
```

- [ ] **Step 4: Run — expect pass**

```bash
pytest tests/test_tokens.py -v
```
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/wswdy/tokens.py tests/test_tokens.py
git commit -m "feat(tokens): HMAC-signed tokens with purpose + optional expiry"
```

---

### Task 6: Short ID generator (`src/wswdy/ids.py`)

**Files:**
- Create: `src/wswdy/ids.py`, `tests/test_ids.py`

- [ ] **Step 1: Write the failing test**

`tests/test_ids.py`:
```python
import re
from wswdy.ids import new_subscriber_id


def test_new_subscriber_id_shape():
    sid = new_subscriber_id()
    assert isinstance(sid, str)
    assert 6 <= len(sid) <= 16
    assert re.fullmatch(r"[A-Za-z0-9_-]+", sid)


def test_new_subscriber_id_unique():
    ids = {new_subscriber_id() for _ in range(2000)}
    assert len(ids) == 2000
```

- [ ] **Step 2: Run — expect failure, then implement**

`src/wswdy/ids.py`:
```python
"""Short URL-safe random IDs for subscribers."""
import secrets


def new_subscriber_id() -> str:
    """Return ~43 bits of entropy as an 8-char URL-safe string."""
    return secrets.token_urlsafe(6)
```

- [ ] **Step 3: Run — expect pass; commit**

```bash
pytest tests/test_ids.py -v
git add src/wswdy/ids.py tests/test_ids.py
git commit -m "feat(ids): URL-safe subscriber ID generator"
```

---

### Task 7: Geo helpers (`src/wswdy/geo.py`)

**Files:**
- Create: `src/wswdy/geo.py`, `tests/test_geo.py`

- [ ] **Step 1: Write the failing test**

`tests/test_geo.py`:
```python
import pytest
from wswdy.geo import haversine_m, in_dc_bbox, DC_BBOX


def test_haversine_zero_distance():
    assert haversine_m(38.9, -77.0, 38.9, -77.0) == pytest.approx(0.0, abs=0.5)


def test_haversine_known_distance():
    # Logan Circle (38.9097,-77.0319) to Lincoln Memorial (38.8893,-77.0502) ~ 2.65 km
    d = haversine_m(38.9097, -77.0319, 38.8893, -77.0502)
    assert 2400 <= d <= 2900


def test_in_dc_bbox_logan_circle():
    assert in_dc_bbox(38.9097, -77.0319) is True


def test_in_dc_bbox_baltimore_no():
    assert in_dc_bbox(39.29, -76.62) is False


def test_in_dc_bbox_alexandria_no():
    # Alexandria is just south of DC
    assert in_dc_bbox(38.80, -77.05) is False


def test_dc_bbox_constant_shape():
    assert DC_BBOX == (38.791, -77.120, 38.996, -76.909)
```

- [ ] **Step 2: Run — expect failure, then implement**

`src/wswdy/geo.py`:
```python
"""Geographic helpers — haversine distance and DC bounding-box check."""
import math
from typing import Final

# (south_lat, west_lon, north_lat, east_lon) — DC's official boundary in WGS84.
DC_BBOX: Final[tuple[float, float, float, float]] = (38.791, -77.120, 38.996, -76.909)

_EARTH_RADIUS_M: Final = 6_371_000.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = rlat2 - rlat1
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(a))


def in_dc_bbox(lat: float, lon: float) -> bool:
    s, w, n, e = DC_BBOX
    return s <= lat <= n and w <= lon <= e
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_geo.py -v
git add src/wswdy/geo.py tests/test_geo.py
git commit -m "feat(geo): haversine distance + DC bbox check"
```

---

**End of Phase 1.** At this point: project compiles, all foundational pure-function utilities are tested, the schema can be created.

---

## Phase 2 — Data repositories (Tasks 8–12)

Each repo owns SQL for its table and returns plain `dict` rows (or lists thereof). No SQL outside `repos/`.

### Task 8: Subscribers repo (`src/wswdy/repos/subscribers.py`)

**Files:**
- Create: `src/wswdy/repos/__init__.py`, `src/wswdy/repos/subscribers.py`, `tests/test_repos_subscribers.py`

- [ ] **Step 1: Create empty package init**

```bash
mkdir -p src/wswdy/repos
echo '"""Repositories — SQL behind a thin API."""' > src/wswdy/repos/__init__.py
```

- [ ] **Step 2: Write the failing test**

`tests/test_repos_subscribers.py`:
```python
import pytest
from wswdy.repos.subscribers import (
    insert_pending, get, set_status, set_last_sent, list_active, list_by_status,
)


def _new(db, **overrides):
    args = dict(
        sid="abc12345",
        display_name="Jane",
        email="jane@example.com",
        phone=None,
        preferred_channel="email",
        address_text="1500 14th St NW",
        lat=38.9097,
        lon=-77.0319,
        radius_m=1000,
    )
    args.update(overrides)
    return insert_pending(db, **args)


def test_insert_and_get(db):
    _new(db)
    s = get(db, "abc12345")
    assert s["display_name"] == "Jane"
    assert s["status"] == "PENDING"
    assert s["preferred_channel"] == "email"


def test_get_missing_returns_none(db):
    assert get(db, "nope") is None


def test_set_status_to_approved_stamps_approved_at(db):
    _new(db)
    set_status(db, "abc12345", "APPROVED")
    s = get(db, "abc12345")
    assert s["status"] == "APPROVED"
    assert s["approved_at"] is not None


def test_set_status_to_unsubscribed_stamps_unsubscribed_at(db):
    _new(db)
    set_status(db, "abc12345", "APPROVED")
    set_status(db, "abc12345", "UNSUBSCRIBED")
    s = get(db, "abc12345")
    assert s["unsubscribed_at"] is not None


def test_set_last_sent(db):
    _new(db)
    set_last_sent(db, "abc12345", "2026-04-28T10:00:00Z")
    s = get(db, "abc12345")
    assert s["last_sent_at"] == "2026-04-28T10:00:00Z"


def test_list_active_only_returns_approved(db):
    _new(db, sid="a")
    _new(db, sid="b")
    _new(db, sid="c")
    set_status(db, "a", "APPROVED")
    set_status(db, "b", "APPROVED")
    set_status(db, "b", "UNSUBSCRIBED")
    actives = list_active(db)
    assert [s["id"] for s in actives] == ["a"]


def test_invalid_status_raises(db):
    _new(db)
    with pytest.raises(ValueError):
        set_status(db, "abc12345", "WHATEVER")
```

- [ ] **Step 3: Run — expect ImportError; implement**

`src/wswdy/repos/subscribers.py`:
```python
"""Subscriber CRUD."""
import sqlite3
from datetime import datetime, timezone

VALID_STATUSES = {"PENDING", "APPROVED", "REJECTED", "UNSUBSCRIBED"}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def insert_pending(
    db: sqlite3.Connection, *,
    sid: str, display_name: str, email: str | None, phone: str | None,
    preferred_channel: str, address_text: str, lat: float, lon: float, radius_m: int,
) -> str:
    db.execute(
        """INSERT INTO subscribers
           (id, display_name, email, phone, preferred_channel,
            address_text, lat, lon, radius_m, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')""",
        (sid, display_name, email, phone, preferred_channel,
         address_text, lat, lon, radius_m),
    )
    db.commit()
    return sid


def get(db: sqlite3.Connection, sid: str) -> dict | None:
    row = db.execute("SELECT * FROM subscribers WHERE id = ?", (sid,)).fetchone()
    return dict(row) if row else None


def set_status(db: sqlite3.Connection, sid: str, status: str) -> None:
    if status not in VALID_STATUSES:
        raise ValueError(f"invalid status: {status}")
    now = _utcnow()
    if status == "APPROVED":
        db.execute("UPDATE subscribers SET status=?, approved_at=? WHERE id=?",
                   (status, now, sid))
    elif status == "UNSUBSCRIBED":
        db.execute("UPDATE subscribers SET status=?, unsubscribed_at=? WHERE id=?",
                   (status, now, sid))
    else:
        db.execute("UPDATE subscribers SET status=? WHERE id=?", (status, sid))
    db.commit()


def set_last_sent(db: sqlite3.Connection, sid: str, when_iso: str) -> None:
    db.execute("UPDATE subscribers SET last_sent_at=? WHERE id=?", (when_iso, sid))
    db.commit()


def list_active(db: sqlite3.Connection) -> list[dict]:
    rows = db.execute("SELECT * FROM subscribers WHERE status='APPROVED' "
                      "ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def list_by_status(db: sqlite3.Connection, status: str) -> list[dict]:
    if status not in VALID_STATUSES:
        raise ValueError(f"invalid status: {status}")
    rows = db.execute("SELECT * FROM subscribers WHERE status=? ORDER BY created_at DESC",
                      (status,)).fetchall()
    return [dict(r) for r in rows]
```

- [ ] **Step 4: Run — expect pass; commit**

```bash
pytest tests/test_repos_subscribers.py -v
git add src/wswdy/repos tests/test_repos_subscribers.py
git commit -m "feat(repos): subscribers repository"
```

---

### Task 9: Crimes repo (`src/wswdy/repos/crimes.py`)

**Files:**
- Create: `src/wswdy/repos/crimes.py`, `tests/test_repos_crimes.py`

- [ ] **Step 1: Write the failing test**

`tests/test_repos_crimes.py`:
```python
from datetime import datetime, timedelta, timezone
from wswdy.repos.crimes import (
    upsert_many, count_in_radius, list_in_radius, list_in_radius_window, prune_older_than,
)


def _crime(ccn, offense="THEFT/OTHER", method=None, lat=38.9097, lon=-77.0319,
           when=None, raw=None):
    return {
        "ccn": ccn, "offense": offense, "method": method, "shift": "DAY",
        "block_address": "1400 block of P St NW",
        "lat": lat, "lon": lon,
        "report_dt": when or "2026-04-27T12:00:00Z",
        "start_dt": None, "end_dt": None,
        "ward": "2", "district": "THIRD",
        "raw_json": raw or "{}",
    }


def test_upsert_inserts_new(db):
    n_added, n_updated = upsert_many(db, [_crime("C1"), _crime("C2")])
    assert (n_added, n_updated) == (2, 0)


def test_upsert_updates_existing_on_same_ccn(db):
    upsert_many(db, [_crime("C1", offense="THEFT/OTHER")])
    n_added, n_updated = upsert_many(db, [_crime("C1", offense="ARSON")])
    assert (n_added, n_updated) == (0, 1)
    rows = db.execute("SELECT offense FROM crimes WHERE ccn='C1'").fetchall()
    assert rows[0]["offense"] == "ARSON"


def test_count_in_radius(db):
    upsert_many(db, [
        _crime("near1", lat=38.9097, lon=-77.0319),                       # 0 m
        _crime("near2", lat=38.9100, lon=-77.0319),                       # ~33 m
        _crime("far",   lat=38.9300, lon=-77.0500),                       # ~3 km
    ])
    n = count_in_radius(db, 38.9097, -77.0319, 500)
    assert n == 2


def test_list_in_radius_filters_correctly(db):
    upsert_many(db, [
        _crime("a", lat=38.9097, lon=-77.0319, when="2026-04-27T08:00:00Z"),
        _crime("b", lat=38.9099, lon=-77.0319, when="2026-04-27T09:00:00Z"),
        _crime("c", lat=38.9500, lon=-77.0500, when="2026-04-27T10:00:00Z"),
    ])
    rows = list_in_radius(db, 38.9097, -77.0319, 500)
    ccns = {r["ccn"] for r in rows}
    assert ccns == {"a", "b"}


def test_list_in_radius_window_24h(db):
    now = datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc)
    upsert_many(db, [
        _crime("recent", when=(now - timedelta(hours=2)).isoformat()),
        _crime("oldish", when=(now - timedelta(hours=30)).isoformat()),
        _crime("ancient", when=(now - timedelta(days=10)).isoformat()),
    ])
    rows = list_in_radius_window(db, 38.9097, -77.0319, 500,
                                 start=(now - timedelta(hours=24)).isoformat(),
                                 end=now.isoformat())
    assert {r["ccn"] for r in rows} == {"recent"}


def test_prune_deletes_old(db):
    old = "2025-01-01T00:00:00Z"
    new = "2026-04-27T00:00:00Z"
    upsert_many(db, [_crime("old", when=old), _crime("new", when=new)])
    deleted = prune_older_than(db, "2026-01-01T00:00:00Z")
    assert deleted == 1
    remaining = db.execute("SELECT ccn FROM crimes").fetchall()
    assert [r["ccn"] for r in remaining] == ["new"]
```

- [ ] **Step 2: Implement**

`src/wswdy/repos/crimes.py`:
```python
"""Crimes table — upsert + radius-filtered queries.

Distance filter uses an equirectangular approximation pre-filter (cheap, in SQL),
followed by an exact haversine refinement in Python on the small candidate set.
At <100k crimes this is well under a millisecond.
"""
import math
import sqlite3

from wswdy.geo import haversine_m

# 1 degree latitude  ≈ 111_320 m
# 1 degree longitude ≈ 111_320 * cos(lat) m  (varies with latitude — DC ≈ 86_700 m)
_M_PER_DEG_LAT = 111_320.0


def _bbox(lat: float, lon: float, radius_m: float) -> tuple[float, float, float, float]:
    dlat = radius_m / _M_PER_DEG_LAT
    dlon = radius_m / (_M_PER_DEG_LAT * math.cos(math.radians(lat)))
    return lat - dlat, lat + dlat, lon - dlon, lon + dlon


def upsert_many(db: sqlite3.Connection, crimes: list[dict]) -> tuple[int, int]:
    """Returns (n_added, n_updated)."""
    added = updated = 0
    for c in crimes:
        cur = db.execute("SELECT 1 FROM crimes WHERE ccn=?", (c["ccn"],)).fetchone()
        if cur:
            db.execute(
                """UPDATE crimes SET
                   offense=?, method=?, shift=?, block_address=?, lat=?, lon=?,
                   report_dt=?, start_dt=?, end_dt=?, ward=?, district=?, raw_json=?
                   WHERE ccn=?""",
                (c["offense"], c["method"], c["shift"], c["block_address"], c["lat"], c["lon"],
                 c["report_dt"], c["start_dt"], c["end_dt"], c["ward"], c["district"],
                 c["raw_json"], c["ccn"]),
            )
            updated += 1
        else:
            db.execute(
                """INSERT INTO crimes
                   (ccn, offense, method, shift, block_address, lat, lon,
                    report_dt, start_dt, end_dt, ward, district, raw_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (c["ccn"], c["offense"], c["method"], c["shift"], c["block_address"],
                 c["lat"], c["lon"], c["report_dt"], c["start_dt"], c["end_dt"],
                 c["ward"], c["district"], c["raw_json"]),
            )
            added += 1
    db.commit()
    return added, updated


def _candidates(db: sqlite3.Connection, lat: float, lon: float, radius_m: float,
                extra_where: str = "", params: tuple = ()) -> list[dict]:
    s_lat, n_lat, w_lon, e_lon = _bbox(lat, lon, radius_m)
    sql = ("SELECT * FROM crimes WHERE lat BETWEEN ? AND ? AND lon BETWEEN ? AND ?"
           + (" AND " + extra_where if extra_where else ""))
    rows = db.execute(sql, (s_lat, n_lat, w_lon, e_lon, *params)).fetchall()
    return [dict(r) for r in rows
            if haversine_m(lat, lon, r["lat"], r["lon"]) <= radius_m]


def count_in_radius(db: sqlite3.Connection, lat: float, lon: float, radius_m: float) -> int:
    return len(_candidates(db, lat, lon, radius_m))


def list_in_radius(db: sqlite3.Connection, lat: float, lon: float, radius_m: float) -> list[dict]:
    return _candidates(db, lat, lon, radius_m)


def list_in_radius_window(db: sqlite3.Connection, lat: float, lon: float, radius_m: float,
                          *, start: str, end: str) -> list[dict]:
    return _candidates(
        db, lat, lon, radius_m,
        extra_where="report_dt >= ? AND report_dt < ?",
        params=(start, end),
    )


def prune_older_than(db: sqlite3.Connection, cutoff_iso: str) -> int:
    cur = db.execute("DELETE FROM crimes WHERE report_dt < ?", (cutoff_iso,))
    db.commit()
    return cur.rowcount
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_repos_crimes.py -v
git add src/wswdy/repos/crimes.py tests/test_repos_crimes.py
git commit -m "feat(repos): crimes repo with radius queries + prune"
```

---

### Task 10: SendLog repo (`src/wswdy/repos/send_log.py`)

**Files:**
- Create: `src/wswdy/repos/send_log.py`, `tests/test_repos_send_log.py`

- [ ] **Step 1: Write the failing test**

`tests/test_repos_send_log.py`:
```python
from wswdy.repos.send_log import record, exists_for_today, recent_failures, send_volume_last_n_days
from wswdy.repos.subscribers import insert_pending


def _sub(db, sid="s1"):
    insert_pending(db, sid=sid, display_name="J", email="j@x.com", phone=None,
                   preferred_channel="email", address_text="x",
                   lat=38.9, lon=-77.0, radius_m=1000)
    return sid


def test_record_and_exists(db):
    _sub(db)
    record(db, "s1", "2026-04-28", "email", "sent")
    assert exists_for_today(db, "s1", "2026-04-28", "email") is True
    assert exists_for_today(db, "s1", "2026-04-28", "whatsapp") is False


def test_record_idempotent_unique_constraint(db):
    _sub(db)
    record(db, "s1", "2026-04-28", "email", "sent")
    # Re-recording the same (sid, date, channel) should be a no-op, not an error.
    record(db, "s1", "2026-04-28", "email", "sent")
    rows = db.execute("SELECT COUNT(*) FROM send_log").fetchone()[0]
    assert rows == 1


def test_recent_failures(db):
    _sub(db, "s1"); _sub(db, "s2")
    record(db, "s1", "2026-04-28", "email", "failed", error="smtp 530")
    record(db, "s2", "2026-04-28", "email", "sent")
    fails = recent_failures(db, limit=10)
    assert len(fails) == 1
    assert fails[0]["subscriber_id"] == "s1"
    assert fails[0]["error"] == "smtp 530"


def test_send_volume_last_n_days(db):
    _sub(db)
    record(db, "s1", "2026-04-26", "email", "sent")
    record(db, "s1", "2026-04-27", "email", "sent")
    record(db, "s1", "2026-04-28", "email", "failed")
    rows = send_volume_last_n_days(db, n=7, today="2026-04-28")
    # rows is a list of dicts with date, sent, failed counts
    by_date = {r["send_date"]: r for r in rows}
    assert by_date["2026-04-26"]["sent"] == 1
    assert by_date["2026-04-28"]["failed"] == 1
```

- [ ] **Step 2: Implement**

`src/wswdy/repos/send_log.py`:
```python
"""Send log — one row per (subscriber, send_date, channel)."""
import sqlite3


def record(db: sqlite3.Connection, subscriber_id: str, send_date: str,
           channel: str, status: str, error: str | None = None) -> None:
    db.execute(
        """INSERT OR IGNORE INTO send_log
           (subscriber_id, send_date, channel, status, error)
           VALUES (?, ?, ?, ?, ?)""",
        (subscriber_id, send_date, channel, status, error),
    )
    db.commit()


def exists_for_today(db: sqlite3.Connection, subscriber_id: str,
                     send_date: str, channel: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM send_log WHERE subscriber_id=? AND send_date=? AND channel=?",
        (subscriber_id, send_date, channel),
    ).fetchone()
    return row is not None


def recent_failures(db: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = db.execute(
        "SELECT * FROM send_log WHERE status='failed' ORDER BY sent_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def send_volume_last_n_days(db: sqlite3.Connection, *, n: int, today: str) -> list[dict]:
    """Returns one row per send_date with sent/failed counts. Empty days omitted."""
    rows = db.execute(
        """SELECT send_date,
                  SUM(CASE WHEN status='sent'   THEN 1 ELSE 0 END) AS sent,
                  SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                  SUM(CASE WHEN status='skipped' THEN 1 ELSE 0 END) AS skipped
             FROM send_log
            WHERE send_date >= date(?, ?)
         GROUP BY send_date
         ORDER BY send_date""",
        (today, f"-{n} days"),
    ).fetchall()
    return [dict(r) for r in rows]
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_repos_send_log.py -v
git add src/wswdy/repos/send_log.py tests/test_repos_send_log.py
git commit -m "feat(repos): send_log repo with idempotent INSERT OR IGNORE"
```

---

### Task 11: FetchLog repo (`src/wswdy/repos/fetch_log.py`)

**Files:**
- Create: `src/wswdy/repos/fetch_log.py`, `tests/test_repos_fetch_log.py`

- [ ] **Step 1: Write the failing test**

`tests/test_repos_fetch_log.py`:
```python
from wswdy.repos.fetch_log import record_success, record_failure, last_successful


def test_record_success_and_query(db):
    record_success(db, added=10, updated=2)
    last = last_successful(db)
    assert last["status"] == "ok"
    assert last["crimes_added"] == 10
    assert last["crimes_updated"] == 2


def test_record_failure(db):
    record_failure(db, error="boom")
    last = last_successful(db)
    assert last is None  # there is no successful fetch on record


def test_last_successful_returns_most_recent(db):
    record_success(db, added=1, updated=0)
    record_failure(db, error="x")
    record_success(db, added=5, updated=1)
    last = last_successful(db)
    assert last["crimes_added"] == 5
```

- [ ] **Step 2: Implement**

`src/wswdy/repos/fetch_log.py`:
```python
"""Fetch log — one row per MPD fetch attempt."""
import sqlite3


def record_success(db: sqlite3.Connection, *, added: int, updated: int) -> None:
    db.execute(
        "INSERT INTO fetch_log (status, crimes_added, crimes_updated) VALUES ('ok', ?, ?)",
        (added, updated),
    )
    db.commit()


def record_failure(db: sqlite3.Connection, *, error: str) -> None:
    db.execute(
        "INSERT INTO fetch_log (status, error) VALUES ('failed', ?)",
        (error,),
    )
    db.commit()


def last_successful(db: sqlite3.Connection) -> dict | None:
    row = db.execute(
        "SELECT * FROM fetch_log WHERE status='ok' ORDER BY fetched_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def last_attempt(db: sqlite3.Connection) -> dict | None:
    row = db.execute(
        "SELECT * FROM fetch_log ORDER BY fetched_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_repos_fetch_log.py -v
git add src/wswdy/repos/fetch_log.py tests/test_repos_fetch_log.py
git commit -m "feat(repos): fetch_log repo"
```

---

### Task 12: AdminAlerts repo (`src/wswdy/repos/admin_alerts.py`)

**Files:**
- Create: `src/wswdy/repos/admin_alerts.py`, `tests/test_repos_admin_alerts.py`

- [ ] **Step 1: Write the failing test**

`tests/test_repos_admin_alerts.py`:
```python
from datetime import datetime, timedelta, timezone
from wswdy.repos.admin_alerts import (
    record, is_suppressed, set_suppressed_until, list_recent,
)


def _now():
    return datetime.now(timezone.utc)


def test_record_creates_row(db):
    record(db, alert_type="mpd_down", message="MPD 503")
    rows = db.execute("SELECT * FROM admin_alerts").fetchall()
    assert len(rows) == 1
    assert rows[0]["alert_type"] == "mpd_down"


def test_is_suppressed_false_when_no_recent(db):
    assert is_suppressed(db, "mpd_down") is False


def test_set_and_check_suppression(db):
    until = (_now() + timedelta(hours=1)).isoformat()
    set_suppressed_until(db, "mpd_down", until)
    assert is_suppressed(db, "mpd_down") is True


def test_suppression_expires(db):
    past = (_now() - timedelta(hours=1)).isoformat()
    set_suppressed_until(db, "mpd_down", past)
    assert is_suppressed(db, "mpd_down") is False


def test_list_recent(db):
    record(db, alert_type="x", message="m1")
    record(db, alert_type="y", message="m2")
    rows = list_recent(db, limit=10)
    assert len(rows) == 2
```

- [ ] **Step 2: Implement**

`src/wswdy/repos/admin_alerts.py`:
```python
"""Admin alerts — log of alerts sent + 6h suppression markers."""
import sqlite3
from datetime import datetime, timezone


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def record(db: sqlite3.Connection, *, alert_type: str, message: str) -> int:
    cur = db.execute(
        "INSERT INTO admin_alerts (alert_type, message) VALUES (?, ?)",
        (alert_type, message),
    )
    db.commit()
    return cur.lastrowid


def set_suppressed_until(db: sqlite3.Connection, alert_type: str, until_iso: str) -> None:
    """Marks all subsequent alerts of `alert_type` as suppressed until `until_iso`."""
    db.execute(
        "INSERT INTO admin_alerts (alert_type, message, suppressed_until) VALUES (?, ?, ?)",
        (alert_type, "(suppression marker)", until_iso),
    )
    db.commit()


def is_suppressed(db: sqlite3.Connection, alert_type: str) -> bool:
    row = db.execute(
        """SELECT suppressed_until FROM admin_alerts
            WHERE alert_type=? AND suppressed_until IS NOT NULL
         ORDER BY created_at DESC LIMIT 1""",
        (alert_type,),
    ).fetchone()
    if not row or not row["suppressed_until"]:
        return False
    return row["suppressed_until"] > _utcnow_iso()


def list_recent(db: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = db.execute(
        """SELECT * FROM admin_alerts
            WHERE message != '(suppression marker)'
         ORDER BY created_at DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_repos_admin_alerts.py -v
git add src/wswdy/repos/admin_alerts.py tests/test_repos_admin_alerts.py
git commit -m "feat(repos): admin_alerts repo with suppression markers"
```

**End of Phase 2.** Persistence layer is complete and fully tested.

---

## Phase 3 — External clients (Tasks 13–15)

Each client wraps one external service and is independently testable with `respx` (HTTP mock) or fixture files.

### Task 13: MPD GeoJSON fetcher (`src/wswdy/clients/mpd.py`)

**Files:**
- Create: `src/wswdy/clients/__init__.py`, `src/wswdy/clients/mpd.py`, `tests/test_mpd.py`, `tests/fixtures/mpd_sample.geojson`

- [ ] **Step 1: Save a real fixture from the live feed**

```bash
mkdir -p tests/fixtures src/wswdy/clients
echo '"""External service clients."""' > src/wswdy/clients/__init__.py
curl -s "https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/FeatureServer/39/query?outFields=*&where=1%3D1&resultRecordCount=20&f=geojson" \
  -o tests/fixtures/mpd_sample.geojson
test -s tests/fixtures/mpd_sample.geojson && echo "fixture saved" || echo "FAILED"
```

If the live feed is unreachable, hand-author a minimal valid fixture with 3 features matching the schema in step 3.

- [ ] **Step 2: Write the failing test**

`tests/test_mpd.py`:
```python
import json
from pathlib import Path
import httpx
import pytest
import respx
from wswdy.clients.mpd import fetch_recent_geojson, parse_features


FIXTURE = Path(__file__).parent / "fixtures" / "mpd_sample.geojson"


@respx.mock
async def test_fetch_recent_geojson_returns_dict():
    respx.get("https://example.test/feed").mock(
        return_value=httpx.Response(200, content=FIXTURE.read_bytes(),
                                    headers={"content-type": "application/json"})
    )
    out = await fetch_recent_geojson("https://example.test/feed")
    assert out["type"] == "FeatureCollection"
    assert "features" in out


@respx.mock
async def test_fetch_recent_geojson_raises_on_500():
    respx.get("https://example.test/feed").mock(return_value=httpx.Response(500))
    with pytest.raises(httpx.HTTPStatusError):
        await fetch_recent_geojson("https://example.test/feed")


def test_parse_features_extracts_required_fields():
    data = json.loads(FIXTURE.read_text())
    crimes = parse_features(data)
    assert len(crimes) > 0
    c = crimes[0]
    # required keys for upsert_many
    for k in ("ccn", "offense", "method", "shift", "block_address",
              "lat", "lon", "report_dt", "start_dt", "end_dt",
              "ward", "district", "raw_json"):
        assert k in c, f"missing key: {k}"
    assert isinstance(c["lat"], float)
    assert isinstance(c["lon"], float)
    assert c["raw_json"].startswith("{")


def test_parse_features_skips_features_with_no_geometry():
    crimes = parse_features({
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "geometry": None, "properties": {"CCN": "X"}},
            {"type": "Feature", "geometry": {"type": "Point", "coordinates": [-77.0, 38.9]},
             "properties": {"CCN": "Y", "OFFENSE": "ROBBERY", "METHOD": "GUN",
                            "SHIFT": "DAY", "BLOCK": "x", "REPORT_DAT": 1714150000000,
                            "START_DATE": 1714150000000, "END_DATE": None,
                            "WARD": "2", "DISTRICT": "3"}},
        ],
    })
    assert [c["ccn"] for c in crimes] == ["Y"]


def test_parse_features_handles_epoch_ms_timestamps():
    crimes = parse_features({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-77.03, 38.91]},
            "properties": {
                "CCN": "T1", "OFFENSE": "BURGLARY", "METHOD": None, "SHIFT": "DAY",
                "BLOCK": "1500 BLOCK", "REPORT_DAT": 1714150000000,
                "START_DATE": 1714150000000, "END_DATE": None,
                "WARD": "2", "DISTRICT": "3",
            },
        }],
    })
    assert crimes[0]["report_dt"].startswith("2024-")  # 1714150000000 = 2024-04-26
```

- [ ] **Step 3: Implement**

`src/wswdy/clients/mpd.py`:
```python
"""MPD GeoJSON fetcher.

The MPD feed publishes one Feature per reported incident. Coordinates are
WGS84 (`Point`, [lon, lat]). Timestamp fields are Unix epoch *milliseconds*
from the ArcGIS server.
"""
import json
from datetime import datetime, timezone
from typing import Any
import httpx


async def fetch_recent_geojson(feed_url: str, *, timeout_s: float = 30.0) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(feed_url)
        r.raise_for_status()
        return r.json()


def _epoch_ms_to_iso(v: Any) -> str | None:
    if v is None:
        return None
    try:
        ms = int(v)
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat(timespec="seconds")
    except (TypeError, ValueError):
        return None


def parse_features(geojson: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for feat in geojson.get("features") or []:
        geom = feat.get("geometry")
        if not geom or geom.get("type") != "Point":
            continue
        coords = geom.get("coordinates") or [None, None]
        lon, lat = coords[0], coords[1]
        if lat is None or lon is None:
            continue

        p = feat.get("properties") or {}
        ccn = p.get("CCN")
        if not ccn:
            continue

        out.append({
            "ccn": str(ccn),
            "offense": p.get("OFFENSE") or "UNKNOWN",
            "method": p.get("METHOD"),
            "shift": p.get("SHIFT"),
            "block_address": p.get("BLOCK"),
            "lat": float(lat),
            "lon": float(lon),
            "report_dt": _epoch_ms_to_iso(p.get("REPORT_DAT")),
            "start_dt": _epoch_ms_to_iso(p.get("START_DATE")),
            "end_dt": _epoch_ms_to_iso(p.get("END_DATE")),
            "ward": str(p.get("WARD")) if p.get("WARD") is not None else None,
            "district": str(p.get("DISTRICT")) if p.get("DISTRICT") is not None else None,
            "raw_json": json.dumps(p, separators=(",", ":")),
        })
    # Drop any with bad timestamps — MPD occasionally publishes nulls
    return [c for c in out if c["report_dt"]]
```

- [ ] **Step 4: Run; commit**

```bash
pytest tests/test_mpd.py -v
git add src/wswdy/clients tests/test_mpd.py tests/fixtures/mpd_sample.geojson
git commit -m "feat(mpd): GeoJSON fetcher + feature parser with epoch-ms handling"
```

---

### Task 14: MapTiler client (`src/wswdy/clients/maptiler.py`)

**Files:**
- Create: `src/wswdy/clients/maptiler.py`, `tests/test_maptiler.py`

- [ ] **Step 1: Write the failing test**

`tests/test_maptiler.py`:
```python
import json
import httpx
import pytest
import respx
from pathlib import Path
from wswdy.clients.maptiler import (
    geocode_address, GeocodeError, render_static_map,
)


@respx.mock
async def test_geocode_returns_lat_lon_for_dc():
    respx.get(host="api.maptiler.com").mock(return_value=httpx.Response(200, json={
        "features": [{
            "place_name": "1500 14th St NW, Washington, DC, USA",
            "center": [-77.0319, 38.9097],
            "context": [{"id": "region", "text": "District of Columbia"}],
        }],
    }))
    out = await geocode_address("1500 14th St NW", api_key="K")
    assert out["lat"] == pytest.approx(38.9097)
    assert out["lon"] == pytest.approx(-77.0319)
    assert "Washington" in out["display"]


@respx.mock
async def test_geocode_no_results_raises():
    respx.get(host="api.maptiler.com").mock(return_value=httpx.Response(200, json={"features": []}))
    with pytest.raises(GeocodeError, match="no results"):
        await geocode_address("not a real address xyzzy", api_key="K")


@respx.mock
async def test_geocode_outside_dc_raises():
    respx.get(host="api.maptiler.com").mock(return_value=httpx.Response(200, json={
        "features": [{"place_name": "Baltimore, MD", "center": [-76.62, 39.29]}],
    }))
    with pytest.raises(GeocodeError, match="outside DC"):
        await geocode_address("Baltimore", api_key="K")


@respx.mock
async def test_render_static_map_writes_png(tmp_path):
    respx.get(host="api.maptiler.com").mock(
        return_value=httpx.Response(200, content=b"\x89PNG\r\n\x1a\n" + b"\x00" * 100,
                                    headers={"content-type": "image/png"})
    )
    out = tmp_path / "preview.png"
    await render_static_map(
        api_key="K", center_lat=38.9, center_lon=-77.0, radius_m=1000,
        markers=[(38.91, -77.03, 1), (38.90, -77.02, 4)],
        out_path=out, width=600, height=400,
    )
    assert out.exists()
    assert out.read_bytes().startswith(b"\x89PNG")
```

- [ ] **Step 2: Implement**

`src/wswdy/clients/maptiler.py`:
```python
"""MapTiler API client — Geocoding + Static Maps."""
from pathlib import Path
import httpx

from wswdy.geo import in_dc_bbox

GEOCODE_URL = "https://api.maptiler.com/geocoding/{q}.json"
STATIC_URL = "https://api.maptiler.com/maps/streets-v2/static/{lon},{lat},{zoom}/{w}x{h}.png"


class GeocodeError(Exception):
    """Raised when an address can't be resolved or is outside DC."""


async def geocode_address(query: str, *, api_key: str, timeout_s: float = 10.0) -> dict:
    params = {"key": api_key, "limit": 1, "country": "us",
              "bbox": "-77.120,38.791,-76.909,38.996"}  # DC bbox prefilter
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(GEOCODE_URL.format(q=query), params=params)
        r.raise_for_status()
        data = r.json()

    features = data.get("features") or []
    if not features:
        raise GeocodeError("no results for that address")
    f = features[0]
    lon, lat = f["center"]
    if not in_dc_bbox(lat, lon):
        raise GeocodeError("address is outside DC")
    return {"lat": float(lat), "lon": float(lon), "display": f.get("place_name", query)}


def _zoom_for_radius_m(radius_m: int) -> int:
    # Heuristic zoom levels matching the radius circle reasonably on a 600x400 canvas
    if radius_m <= 300: return 16
    if radius_m <= 700: return 15
    if radius_m <= 1300: return 14
    if radius_m <= 2200: return 13
    return 12


_TIER_HEX = {1: "DC2626", 2: "EA580C", 3: "D97706", 4: "65A30D"}


async def render_static_map(
    *, api_key: str,
    center_lat: float, center_lon: float, radius_m: int,
    markers: list[tuple[float, float, int]],
    out_path: Path, width: int = 600, height: int = 400,
    timeout_s: float = 20.0,
) -> Path:
    """Renders a static PNG map with tier-coloured pin markers and writes it to `out_path`.

    `markers` is a list of (lat, lon, tier).
    """
    zoom = _zoom_for_radius_m(radius_m)
    url = STATIC_URL.format(lon=center_lon, lat=center_lat, zoom=zoom, w=width, h=height)
    # MapTiler static API supports `marker` query params: `marker=lon,lat,color`
    params: list[tuple[str, str]] = [("key", api_key)]
    # Home pin first (black-ish)
    params.append(("marker", f"{center_lon},{center_lat},#0A0A0A"))
    for lat, lon, tier in markers[:60]:  # cap to keep URL length sane
        params.append(("marker", f"{lon},{lat},#{_TIER_HEX[tier]}"))
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(r.content)
    return out_path
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_maptiler.py -v
git add src/wswdy/clients/maptiler.py tests/test_maptiler.py
git commit -m "feat(maptiler): geocoding + static map rendering"
```

---

### Task 15: WhatsApp MCP client (`src/wswdy/clients/whatsapp_mcp.py`)

The MCP exposes an HTTP endpoint that accepts `{to, text, image_path?}` and returns `{status: ok|failed, error?}`. The exact contract is determined by the existing MCP — this client wraps it with retries and error classification.

**Files:**
- Create: `src/wswdy/clients/whatsapp_mcp.py`, `tests/test_whatsapp_mcp.py`

- [ ] **Step 1: Write the failing test**

`tests/test_whatsapp_mcp.py`:
```python
import httpx
import pytest
import respx
from wswdy.clients.whatsapp_mcp import (
    send_message, McpUnreachable, McpSessionExpired,
)


@respx.mock
async def test_send_message_ok():
    respx.post("https://mcp.test/send").mock(return_value=httpx.Response(200, json={"status": "ok"}))
    out = await send_message(base_url="https://mcp.test", token="t",
                             to="+12025551234", text="hi", image_path=None)
    assert out["status"] == "ok"


@respx.mock
async def test_send_message_session_expired():
    respx.post("https://mcp.test/send").mock(
        return_value=httpx.Response(401, json={"error": "session_expired"})
    )
    with pytest.raises(McpSessionExpired):
        await send_message(base_url="https://mcp.test", token="t",
                           to="+12025551234", text="hi")


@respx.mock
async def test_send_message_unreachable():
    respx.post("https://mcp.test/send").mock(side_effect=httpx.ConnectError("nope"))
    with pytest.raises(McpUnreachable):
        await send_message(base_url="https://mcp.test", token="t",
                           to="+12025551234", text="hi")


@respx.mock
async def test_send_message_attaches_image(tmp_path):
    img = tmp_path / "x.png"
    img.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.content
        return httpx.Response(200, json={"status": "ok"})

    respx.post("https://mcp.test/send").mock(side_effect=handler)
    await send_message(base_url="https://mcp.test", token="t",
                       to="+12025551234", text="hi", image_path=img)
    assert b"x.png" in captured["body"] or b"image" in captured["body"]
```

- [ ] **Step 2: Implement**

`src/wswdy/clients/whatsapp_mcp.py`:
```python
"""HTTP client for the WhatsApp MCP bridge in the adjacent LXC.

Contract:
  POST {base_url}/send
  Headers: Authorization: Bearer <token>
  Body (multipart if image_path is given, else json):
    {to, text}                                     (json)
    {to, text, image (file)}                       (multipart)
  Returns 200 {status: "ok"}            on success
          401 {error: "session_expired"} when WhatsApp Web session is gone
          5xx / connection error         otherwise
"""
from pathlib import Path
import httpx


class McpUnreachable(Exception):
    """Raised when the MCP service is unreachable (network or 5xx)."""


class McpSessionExpired(Exception):
    """Raised when the MCP returns 401/session_expired — needs QR re-scan."""


async def send_message(
    *, base_url: str, token: str,
    to: str, text: str, image_path: Path | None = None,
    timeout_s: float = 30.0,
) -> dict:
    url = base_url.rstrip("/") + "/send"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            if image_path is not None:
                with image_path.open("rb") as f:
                    files = {"image": (image_path.name, f, "image/png")}
                    data = {"to": to, "text": text}
                    r = await client.post(url, headers=headers, data=data, files=files)
            else:
                r = await client.post(url, headers=headers, json={"to": to, "text": text})
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
        raise McpUnreachable(str(e)) from e

    if r.status_code == 401:
        raise McpSessionExpired(r.text)
    if r.status_code >= 500:
        raise McpUnreachable(f"{r.status_code}: {r.text}")
    r.raise_for_status()
    return r.json()
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_whatsapp_mcp.py -v
git add src/wswdy/clients/whatsapp_mcp.py tests/test_whatsapp_mcp.py
git commit -m "feat(whatsapp): MCP HTTP client with session-expired classification"
```

**End of Phase 3.** All external service integrations live behind testable, mockable clients.

---

## Phase 4 — Notifier system + admin alerter (Tasks 16–19)

### Task 16: Notifier protocol + FakeNotifier (`src/wswdy/notifiers/base.py`, `fake.py`)

**Files:**
- Create: `src/wswdy/notifiers/__init__.py`, `src/wswdy/notifiers/base.py`, `src/wswdy/notifiers/fake.py`, `tests/test_notifiers_fake.py`

- [ ] **Step 1: Create package init**

```bash
mkdir -p src/wswdy/notifiers
echo '"""Notifier abstractions and concrete implementations."""' > src/wswdy/notifiers/__init__.py
```

- [ ] **Step 2: Write the failing test**

`tests/test_notifiers_fake.py`:
```python
from pathlib import Path
import pytest
from wswdy.notifiers.fake import FakeNotifier
from wswdy.notifiers.base import SendResult


async def test_fake_notifier_records_sends():
    n = FakeNotifier()
    r = await n.send(recipient="x@y.com", subject="hi", text="body", image_path=None)
    assert isinstance(r, SendResult)
    assert r.ok is True
    assert n.sent == [{"recipient": "x@y.com", "subject": "hi",
                       "text": "body", "image_path": None}]


async def test_fake_notifier_can_be_set_to_fail():
    n = FakeNotifier(fail_with="boom")
    r = await n.send(recipient="x@y.com", subject="s", text="t", image_path=None)
    assert r.ok is False
    assert r.error == "boom"
```

- [ ] **Step 3: Implement base + fake**

`src/wswdy/notifiers/base.py`:
```python
"""Notifier protocol and supporting types."""
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class SendResult:
    ok: bool
    error: str | None = None
    detail: str | None = None  # provider-specific detail (message id, etc.)


@runtime_checkable
class Notifier(Protocol):
    async def send(self, *, recipient: str, subject: str, text: str,
                   image_path: Path | None) -> SendResult: ...
```

`src/wswdy/notifiers/fake.py`:
```python
"""In-memory notifier for tests."""
from pathlib import Path
from wswdy.notifiers.base import Notifier, SendResult


class FakeNotifier(Notifier):
    def __init__(self, fail_with: str | None = None):
        self.sent: list[dict] = []
        self.fail_with = fail_with

    async def send(self, *, recipient: str, subject: str, text: str,
                   image_path: Path | None) -> SendResult:
        self.sent.append({"recipient": recipient, "subject": subject,
                          "text": text, "image_path": image_path})
        if self.fail_with:
            return SendResult(ok=False, error=self.fail_with)
        return SendResult(ok=True)
```

- [ ] **Step 4: Run; commit**

```bash
pytest tests/test_notifiers_fake.py -v
git add src/wswdy/notifiers tests/test_notifiers_fake.py
git commit -m "feat(notifiers): protocol + SendResult + FakeNotifier"
```

---

### Task 17: EmailNotifier (`src/wswdy/notifiers/email.py`)

**Files:**
- Create: `src/wswdy/notifiers/email.py`, `tests/test_notifiers_email.py`

- [ ] **Step 1: Write the failing test**

`tests/test_notifiers_email.py`:
```python
from pathlib import Path
from unittest.mock import AsyncMock, patch
import pytest
from wswdy.notifiers.email import EmailNotifier


@patch("wswdy.notifiers.email.aiosmtplib.send", new_callable=AsyncMock)
async def test_email_send_ok(mock_send):
    mock_send.return_value = ({}, "ok")
    n = EmailNotifier(host="smtp.test", port=587, user="u", password="p",
                     sender="from@x")
    r = await n.send(recipient="to@y", subject="s", text="t", image_path=None)
    assert r.ok is True
    args, kwargs = mock_send.call_args
    assert kwargs["hostname"] == "smtp.test"
    msg = args[0]
    assert msg["To"] == "to@y"
    assert msg["From"] == "from@x"
    assert msg["Subject"] == "s"


@patch("wswdy.notifiers.email.aiosmtplib.send", new_callable=AsyncMock)
async def test_email_send_with_inline_image(mock_send, tmp_path):
    img = tmp_path / "preview.png"
    img.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 50)
    mock_send.return_value = ({}, "ok")
    n = EmailNotifier(host="smtp.test", port=587, user="u", password="p", sender="f@x")
    await n.send(recipient="to@y", subject="s", text="hello", image_path=img)
    msg = mock_send.call_args.args[0]
    # Walk the multipart and ensure an image part exists
    parts = list(msg.walk())
    assert any(p.get_content_type() == "image/png" for p in parts)


@patch("wswdy.notifiers.email.aiosmtplib.send", new_callable=AsyncMock)
async def test_email_send_failure(mock_send):
    mock_send.side_effect = Exception("connection refused")
    n = EmailNotifier(host="smtp.test", port=587, user="u", password="p", sender="f@x")
    r = await n.send(recipient="to@y", subject="s", text="t", image_path=None)
    assert r.ok is False
    assert "connection refused" in r.error
```

- [ ] **Step 2: Implement**

`src/wswdy/notifiers/email.py`:
```python
"""SMTP-backed notifier."""
from email.message import EmailMessage
from pathlib import Path
import aiosmtplib

from wswdy.notifiers.base import Notifier, SendResult


class EmailNotifier(Notifier):
    def __init__(self, *, host: str, port: int, user: str, password: str,
                 sender: str, use_starttls: bool = True):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.sender = sender
        self.use_starttls = use_starttls

    async def send(self, *, recipient: str, subject: str, text: str,
                   image_path: Path | None) -> SendResult:
        msg = EmailMessage()
        msg["From"] = self.sender
        msg["To"] = recipient
        msg["Subject"] = subject

        if image_path is not None:
            # Multipart: HTML body referencing inline image via cid:preview
            html = (
                f"<html><body style='font-family: -apple-system, system-ui, sans-serif;"
                f" background:#FAFAF6; padding:24px;'>"
                f"<pre style='font: 14px/1.5 ui-monospace, monospace; white-space:pre-wrap;"
                f" background:#fff; padding:18px; border:1px solid #E5E3DC; border-radius:10px;'>"
                f"{_escape(text)}</pre>"
                f"<img src='cid:preview' style='display:block;margin-top:12px;"
                f"max-width:100%;border:1px solid #E5E3DC;border-radius:10px;' />"
                f"</body></html>"
            )
            msg.set_content(text)  # plain-text fallback
            msg.add_alternative(html, subtype="html")
            # Attach inline image (cid)
            data = image_path.read_bytes()
            msg.get_payload()[1].add_related(
                data, maintype="image", subtype="png", cid="<preview>",
            )
        else:
            msg.set_content(text)

        try:
            await aiosmtplib.send(
                msg,
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                start_tls=self.use_starttls,
            )
        except Exception as e:
            return SendResult(ok=False, error=str(e))
        return SendResult(ok=True)


def _escape(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_notifiers_email.py -v
git add src/wswdy/notifiers/email.py tests/test_notifiers_email.py
git commit -m "feat(notifiers): EmailNotifier with inline static-map image"
```

---

### Task 18: WhatsAppMcpNotifier + dispatch (`src/wswdy/notifiers/whatsapp.py`, `base.py` extension)

**Files:**
- Create: `src/wswdy/notifiers/whatsapp.py`, `tests/test_notifiers_whatsapp.py`, `tests/test_notifier_dispatch.py`
- Modify: `src/wswdy/notifiers/base.py` (add `dispatch` function)

- [ ] **Step 1: Write the WhatsApp notifier test**

`tests/test_notifiers_whatsapp.py`:
```python
from pathlib import Path
from unittest.mock import AsyncMock, patch
import pytest
from wswdy.clients.whatsapp_mcp import McpSessionExpired, McpUnreachable
from wswdy.notifiers.whatsapp import WhatsAppMcpNotifier


@patch("wswdy.notifiers.whatsapp.send_message", new_callable=AsyncMock)
async def test_whatsapp_ok(mock_send):
    mock_send.return_value = {"status": "ok", "id": "msg_123"}
    n = WhatsAppMcpNotifier(base_url="http://mcp", token="t")
    r = await n.send(recipient="+12025551234", subject="ignored",
                     text="hi", image_path=None)
    assert r.ok is True
    mock_send.assert_called_once()


@patch("wswdy.notifiers.whatsapp.send_message", new_callable=AsyncMock)
async def test_whatsapp_session_expired_returns_special_error(mock_send):
    mock_send.side_effect = McpSessionExpired("session_expired")
    n = WhatsAppMcpNotifier(base_url="http://mcp", token="t")
    r = await n.send(recipient="+12025551234", subject="x", text="y", image_path=None)
    assert r.ok is False
    assert r.error == "session_expired"


@patch("wswdy.notifiers.whatsapp.send_message", new_callable=AsyncMock)
async def test_whatsapp_unreachable_returns_unreachable(mock_send):
    mock_send.side_effect = McpUnreachable("connect refused")
    n = WhatsAppMcpNotifier(base_url="http://mcp", token="t")
    r = await n.send(recipient="+12025551234", subject="x", text="y", image_path=None)
    assert r.ok is False
    assert r.error == "unreachable"
    assert "connect refused" in r.detail
```

- [ ] **Step 2: Implement**

`src/wswdy/notifiers/whatsapp.py`:
```python
"""WhatsApp notifier — wraps the MCP HTTP client into the Notifier protocol."""
from pathlib import Path

from wswdy.clients.whatsapp_mcp import (
    send_message, McpUnreachable, McpSessionExpired,
)
from wswdy.notifiers.base import Notifier, SendResult


class WhatsAppMcpNotifier(Notifier):
    def __init__(self, *, base_url: str, token: str):
        self.base_url = base_url
        self.token = token

    async def send(self, *, recipient: str, subject: str, text: str,
                   image_path: Path | None) -> SendResult:
        # `subject` is ignored — WhatsApp messages have no subject line.
        try:
            res = await send_message(
                base_url=self.base_url, token=self.token,
                to=recipient, text=text, image_path=image_path,
            )
        except McpSessionExpired as e:
            return SendResult(ok=False, error="session_expired", detail=str(e))
        except McpUnreachable as e:
            return SendResult(ok=False, error="unreachable", detail=str(e))
        if res.get("status") != "ok":
            return SendResult(ok=False, error="rejected", detail=str(res))
        return SendResult(ok=True, detail=res.get("id"))
```

- [ ] **Step 3: Write the dispatch test**

`tests/test_notifier_dispatch.py`:
```python
from pathlib import Path
import pytest
from wswdy.notifiers.base import dispatch, SendResult
from wswdy.notifiers.fake import FakeNotifier


SUB = {"id": "s1", "preferred_channel": "whatsapp",
       "phone": "+12025551234", "email": "fall@back.com"}


async def test_dispatch_routes_to_preferred_channel():
    email = FakeNotifier()
    wa = FakeNotifier()
    r = await dispatch(SUB, email_notifier=email, whatsapp_notifier=wa,
                       subject="s", text="t", image_path=None)
    assert r.ok is True
    assert wa.sent and not email.sent
    assert wa.sent[0]["recipient"] == "+12025551234"


async def test_dispatch_falls_back_to_email_on_whatsapp_unreachable():
    email = FakeNotifier()
    wa = FakeNotifier(fail_with="unreachable")
    r = await dispatch(SUB, email_notifier=email, whatsapp_notifier=wa,
                       subject="s", text="t", image_path=None)
    assert r.ok is True
    assert wa.sent and email.sent
    assert email.sent[0]["recipient"] == "fall@back.com"


async def test_dispatch_does_not_fall_back_on_session_expired():
    # Session-expired is operator-actionable; we don't double-send.
    email = FakeNotifier()
    wa = FakeNotifier(fail_with="session_expired")
    r = await dispatch(SUB, email_notifier=email, whatsapp_notifier=wa,
                       subject="s", text="t", image_path=None)
    assert r.ok is False
    assert r.error == "session_expired"
    assert email.sent == []


async def test_dispatch_email_subscriber_uses_email_directly():
    email = FakeNotifier()
    wa = FakeNotifier()
    sub = {**SUB, "preferred_channel": "email"}
    r = await dispatch(sub, email_notifier=email, whatsapp_notifier=wa,
                       subject="s", text="t", image_path=None)
    assert r.ok is True
    assert email.sent and not wa.sent
    assert email.sent[0]["recipient"] == "fall@back.com"


async def test_dispatch_whatsapp_no_email_fallback_returns_failure():
    email = FakeNotifier()
    wa = FakeNotifier(fail_with="unreachable")
    sub = {**SUB, "email": None}
    r = await dispatch(sub, email_notifier=email, whatsapp_notifier=wa,
                       subject="s", text="t", image_path=None)
    assert r.ok is False
    assert r.error == "unreachable"
```

- [ ] **Step 4: Extend `src/wswdy/notifiers/base.py` with `dispatch`**

Append to `src/wswdy/notifiers/base.py`:
```python


async def dispatch(
    subscriber: dict, *,
    email_notifier: Notifier, whatsapp_notifier: Notifier,
    subject: str, text: str, image_path: "Path | None",
) -> SendResult:
    """Sends to the subscriber's preferred channel; falls back to email if WhatsApp
    is unreachable AND email is on file. Does NOT fall back on session_expired
    (that's an operator-actionable problem, not a recipient-specific one)."""
    channel = subscriber["preferred_channel"]
    if channel == "email":
        return await email_notifier.send(
            recipient=subscriber["email"], subject=subject,
            text=text, image_path=image_path,
        )

    # WhatsApp path
    res = await whatsapp_notifier.send(
        recipient=subscriber["phone"], subject=subject,
        text=text, image_path=image_path,
    )
    if res.ok or res.error == "session_expired" or not subscriber.get("email"):
        return res
    # Fall back to email on transient WhatsApp failures only
    return await email_notifier.send(
        recipient=subscriber["email"], subject=subject,
        text=text, image_path=image_path,
    )
```

(Add `from pathlib import Path` to the top of `base.py` if not already present.)

- [ ] **Step 5: Run; commit**

```bash
pytest tests/test_notifiers_whatsapp.py tests/test_notifier_dispatch.py -v
git add src/wswdy/notifiers tests/test_notifiers_whatsapp.py tests/test_notifier_dispatch.py
git commit -m "feat(notifiers): WhatsApp notifier + channel dispatch with email fallback"
```

---

### Task 19: Admin alerter (`src/wswdy/alerts.py`)

**Files:**
- Create: `src/wswdy/alerts.py`, `tests/test_alerts.py`

- [ ] **Step 1: Write the failing test**

`tests/test_alerts.py`:
```python
from datetime import timedelta
from unittest.mock import AsyncMock, patch
import httpx
import pytest
import respx
from wswdy.alerts import AdminAlerter
from wswdy.notifiers.fake import FakeNotifier
from wswdy.repos.admin_alerts import is_suppressed, list_recent


@respx.mock
async def test_alert_sends_email_and_webhook_and_records(db):
    email = FakeNotifier()
    respx.post("https://ha.test/hook").mock(return_value=httpx.Response(200))
    a = AdminAlerter(db=db, email=email, admin_email="admin@x",
                     ha_webhook_url="https://ha.test/hook",
                     suppression_hours=6)
    await a.alert(alert_type="mpd_down", message="MPD 503 for 30min")
    assert email.sent and email.sent[0]["recipient"] == "admin@x"
    assert "MPD 503" in email.sent[0]["text"]
    assert respx.calls.call_count == 1
    assert is_suppressed(db, "mpd_down")
    assert list_recent(db)[0]["alert_type"] == "mpd_down"


@respx.mock
async def test_alert_suppressed_within_window(db):
    email = FakeNotifier()
    respx.post("https://ha.test/hook").mock(return_value=httpx.Response(200))
    a = AdminAlerter(db=db, email=email, admin_email="admin@x",
                     ha_webhook_url="https://ha.test/hook", suppression_hours=6)
    await a.alert(alert_type="mpd_down", message="first")
    await a.alert(alert_type="mpd_down", message="second")  # suppressed
    assert len(email.sent) == 1
    assert respx.calls.call_count == 1


@respx.mock
async def test_alert_distinct_types_are_independent(db):
    email = FakeNotifier()
    respx.post("https://ha.test/hook").mock(return_value=httpx.Response(200))
    a = AdminAlerter(db=db, email=email, admin_email="admin@x",
                     ha_webhook_url="https://ha.test/hook", suppression_hours=6)
    await a.alert(alert_type="mpd_down", message="m1")
    await a.alert(alert_type="whatsapp_session_expired", message="m2")
    assert len(email.sent) == 2


async def test_alert_no_webhook_url_skips_webhook(db):
    email = FakeNotifier()
    a = AdminAlerter(db=db, email=email, admin_email="admin@x",
                     ha_webhook_url="", suppression_hours=6)
    await a.alert(alert_type="x", message="y")
    assert email.sent  # email still sent
```

- [ ] **Step 2: Implement**

`src/wswdy/alerts.py`:
```python
"""Admin alerter — emails iandmuir@gmail.com + posts a Home Assistant webhook,
suppressing repeats of the same alert_type for a configurable window."""
import sqlite3
from datetime import datetime, timedelta, timezone
import httpx

from wswdy.notifiers.base import Notifier
from wswdy.repos.admin_alerts import (
    record, is_suppressed, set_suppressed_until,
)


class AdminAlerter:
    def __init__(self, *, db: sqlite3.Connection, email: Notifier,
                 admin_email: str, ha_webhook_url: str, suppression_hours: int = 6):
        self.db = db
        self.email = email
        self.admin_email = admin_email
        self.ha_webhook_url = ha_webhook_url
        self.suppression_hours = suppression_hours

    async def alert(self, *, alert_type: str, message: str) -> None:
        if is_suppressed(self.db, alert_type):
            return

        record(self.db, alert_type=alert_type, message=message)
        until = (datetime.now(timezone.utc) +
                 timedelta(hours=self.suppression_hours)).isoformat(timespec="seconds")
        set_suppressed_until(self.db, alert_type, until)

        subject = f"[wswdy] {alert_type}"
        text = f"{message}\n\n— wswdy admin alerter\n(suppressed for {self.suppression_hours}h)"
        await self.email.send(recipient=self.admin_email, subject=subject,
                              text=text, image_path=None)

        if self.ha_webhook_url:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await client.post(self.ha_webhook_url,
                                      json={"alert_type": alert_type, "message": message})
            except Exception:
                # HA being down is not itself an alert-worthy condition; we don't
                # want to recurse and email is already gone out.
                pass
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_alerts.py -v
git add src/wswdy/alerts.py tests/test_alerts.py
git commit -m "feat(alerts): admin alerter with email + HA webhook + 6h suppression"
```

**End of Phase 4.** Outbound delivery and admin alerting are complete.

---

## Phase 5 — Digest builder + scheduled jobs (Tasks 20–23)

### Task 20: Digest builder (`src/wswdy/digest.py`)

**Files:**
- Create: `src/wswdy/digest.py`, `tests/test_digest.py`

- [ ] **Step 1: Write the failing test**

`tests/test_digest.py`:
```python
from wswdy.digest import build_digest_text, summarize_by_tier, select_closest


CRIMES = [
    {"offense": "ROBBERY", "method": "GUN", "block_address": "1400 block of P St NW",
     "lat": 38.9117, "lon": -77.0322, "report_dt": "2026-04-27T21:14:00Z"},
    {"offense": "MOTOR VEHICLE THEFT", "method": None,
     "block_address": "1200 block of 12th St NW",
     "lat": 38.9081, "lon": -77.0298, "report_dt": "2026-04-27T02:30:00Z"},
    {"offense": "THEFT F/AUTO", "method": None,
     "block_address": "1500 block of 14th St NW",
     "lat": 38.9100, "lon": -77.0319, "report_dt": "2026-04-27T03:48:00Z"},
]
HOME = (38.9097, -77.0319)


def test_summarize_by_tier_counts():
    s = summarize_by_tier(CRIMES)
    assert s == {1: 1, 2: 0, 3: 1, 4: 1}


def test_select_closest_within_half_radius():
    closest = select_closest(CRIMES, home_lat=HOME[0], home_lon=HOME[1],
                              radius_m=1000, max_items=3)
    # Half-radius is 500m. The theft from auto and the armed robbery are within.
    offenses = [c["offense"] for c in closest]
    assert "ROBBERY" in offenses or "THEFT F/AUTO" in offenses
    assert all(c["distance_m"] <= 500 for c in closest)
    # Sorted by distance ascending
    distances = [c["distance_m"] for c in closest]
    assert distances == sorted(distances)


def test_build_digest_text_includes_all_required_pieces():
    text = build_digest_text(
        display_name="Jane", radius_m=1000, crimes=CRIMES,
        home_lat=HOME[0], home_lon=HOME[1],
        map_url="https://x/map/abc?token=t",
        unsubscribe_url="https://x/u/abc?token=t",
        mpd_warning=False,
    )
    assert "Jane" in text
    assert "1000m" in text or "1,000m" in text
    assert "3 crimes reported" in text
    # tier counts
    assert "1 violent" in text
    assert "0 serious property" in text
    assert "1 vehicle" in text
    assert "1 petty" in text
    assert "https://x/map/abc?token=t" in text
    assert "https://x/u/abc?token=t" in text


def test_build_digest_zero_crimes_uses_quiet_phrasing():
    text = build_digest_text(
        display_name="Jane", radius_m=800, crimes=[],
        home_lat=HOME[0], home_lon=HOME[1],
        map_url="https://x/m", unsubscribe_url="https://x/u",
        mpd_warning=False,
    )
    assert "0 crimes reported" in text or "Quiet" in text or "no incidents" in text.lower()


def test_build_digest_appends_mpd_warning_when_flagged():
    text = build_digest_text(
        display_name="Jane", radius_m=1000, crimes=CRIMES,
        home_lat=HOME[0], home_lon=HOME[1],
        map_url="https://x/m", unsubscribe_url="https://x/u",
        mpd_warning=True,
    )
    assert "MPD data" in text or "delayed" in text.lower()


def test_select_closest_caps_at_max_items():
    many = [
        {"offense": "THEFT/OTHER", "method": None, "block_address": "x",
         "lat": 38.9098 + i * 0.0001, "lon": -77.0319, "report_dt": "2026-04-27T08:00:00Z"}
        for i in range(10)
    ]
    closest = select_closest(many, home_lat=HOME[0], home_lon=HOME[1],
                              radius_m=1000, max_items=3)
    assert len(closest) == 3
```

- [ ] **Step 2: Implement**

`src/wswdy/digest.py`:
```python
"""Digest message builder — produces the WhatsApp/email body text."""
from datetime import datetime
from zoneinfo import ZoneInfo

from wswdy.geo import haversine_m
from wswdy.tiers import classify

ET = ZoneInfo("America/New_York")

_TIER_GLYPH = {1: "🔴", 2: "🟠", 3: "🟡", 4: "🟢"}
_TIER_LABEL = {1: "violent", 2: "serious property", 3: "vehicle", 4: "petty"}


def summarize_by_tier(crimes: list[dict]) -> dict[int, int]:
    counts = {1: 0, 2: 0, 3: 0, 4: 0}
    for c in crimes:
        counts[classify(c["offense"], c.get("method"))] += 1
    return counts


def select_closest(crimes: list[dict], *, home_lat: float, home_lon: float,
                   radius_m: int, max_items: int = 3) -> list[dict]:
    near_threshold = radius_m / 2
    enriched = []
    for c in crimes:
        d = haversine_m(home_lat, home_lon, c["lat"], c["lon"])
        if d <= near_threshold:
            enriched.append({**c, "distance_m": int(round(d))})
    enriched.sort(key=lambda x: x["distance_m"])
    return enriched[:max_items]


def _fmt_time(iso: str) -> str:
    """Render ISO UTC string as 24h ET."""
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(ET)
    return dt.strftime("%H:%M")


def _humanize_offense(offense: str, method: str | None) -> str:
    base = offense.title().replace("F/Auto", "from auto").replace("/Other", "/other")
    if offense.upper() == "ROBBERY" and (method or "").upper() in {"GUN", "KNIFE"}:
        return "Armed robbery"
    return base


def _tier_examples(crimes: list[dict], tier: int) -> str:
    """Produce a brief example list for a tier, e.g. '1 armed robbery, 2 burglary'."""
    by_offense: dict[str, int] = {}
    for c in crimes:
        if classify(c["offense"], c.get("method")) != tier:
            continue
        label = _humanize_offense(c["offense"], c.get("method")).lower()
        by_offense[label] = by_offense.get(label, 0) + 1
    parts = [f"{n} {label}" for label, n in sorted(by_offense.items(),
                                                    key=lambda x: -x[1])]
    return ", ".join(parts)


def build_digest_text(*, display_name: str, radius_m: int, crimes: list[dict],
                     home_lat: float, home_lon: float,
                     map_url: str, unsubscribe_url: str,
                     mpd_warning: bool = False) -> str:
    n = len(crimes)
    counts = summarize_by_tier(crimes)
    radius_str = f"{radius_m:,}m"

    lines: list[str] = []
    lines.append(f"Good morning {display_name} ☀️")
    lines.append("")
    if n == 0:
        lines.append(f"Quiet night — 0 crimes reported within {radius_str} of your home in the last 24 hours.")
    else:
        lines.append(f"In the last 24 hours there were {n} crimes reported within {radius_str} of your home:")
        lines.append("")
        for tier in (1, 2, 3, 4):
            c = counts[tier]
            label = _TIER_LABEL[tier]
            glyph = _TIER_GLYPH[tier]
            examples = _tier_examples(crimes, tier)
            if c == 0:
                lines.append(f"{glyph} 0 {label}")
            else:
                lines.append(f"{glyph} {c} {label}  — {examples}" if examples else f"{glyph} {c} {label}")

    lines.append("")
    closest = select_closest(crimes, home_lat=home_lat, home_lon=home_lon,
                              radius_m=radius_m, max_items=3)
    if closest:
        lines.append("Closest to you:")
        for c in closest:
            offense = _humanize_offense(c["offense"], c.get("method"))
            t = _fmt_time(c["report_dt"])
            lines.append(f"• {offense} — {c['distance_m']}m away ({c['block_address']}, {t})")
    else:
        lines.append("No incidents reported in your immediate vicinity. ✨")

    lines.append("")
    lines.append("📍 View map (last 24h, with toggles for 7d / 30d):")
    lines.append(map_url)
    lines.append("")
    lines.append("Reply STOP or click to unsubscribe:")
    lines.append(unsubscribe_url)

    if mpd_warning:
        lines.append("")
        lines.append("⚠️ MPD data may be delayed — we'll catch you up tomorrow.")

    return "\n".join(lines)
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_digest.py -v
git add src/wswdy/digest.py tests/test_digest.py
git commit -m "feat(digest): message text builder with tier summary + closest-incidents"
```

---

### Task 21: Fetch job (`src/wswdy/jobs/fetch.py`)

**Files:**
- Create: `src/wswdy/jobs/__init__.py`, `src/wswdy/jobs/fetch.py`, `tests/test_jobs_fetch.py`

- [ ] **Step 1: Create init + write the failing test**

```bash
mkdir -p src/wswdy/jobs
echo '"""Scheduled jobs."""' > src/wswdy/jobs/__init__.py
```

`tests/test_jobs_fetch.py`:
```python
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch
import httpx
import pytest
import respx
from wswdy.jobs.fetch import run_fetch
from wswdy.notifiers.fake import FakeNotifier
from wswdy.alerts import AdminAlerter
from wswdy.repos.fetch_log import last_attempt
from wswdy.repos.crimes import count_in_radius


FIXTURE = Path(__file__).parent / "fixtures" / "mpd_sample.geojson"


@respx.mock
async def test_fetch_success_upserts_and_logs(db):
    respx.get("https://feed.test/q").mock(
        return_value=httpx.Response(200, content=FIXTURE.read_bytes())
    )
    email = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="a@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_fetch(db=db, feed_url="https://feed.test/q", alerter=alerter)
    assert out["status"] == "ok"
    assert last_attempt(db)["status"] == "ok"
    # At least one crime upserted
    assert db.execute("SELECT COUNT(*) FROM crimes").fetchone()[0] > 0


@respx.mock
async def test_fetch_retries_on_failure_then_succeeds(db):
    route = respx.get("https://feed.test/q")
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(503),
        httpx.Response(200, content=FIXTURE.read_bytes()),
    ]
    email = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="a@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_fetch(db=db, feed_url="https://feed.test/q", alerter=alerter,
                          retry_delays_s=[0, 0])  # no real sleeping
    assert out["status"] == "ok"
    assert route.call_count == 3


@respx.mock
async def test_fetch_all_attempts_fail_alerts_admin(db):
    respx.get("https://feed.test/q").mock(return_value=httpx.Response(503))
    email = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="a@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_fetch(db=db, feed_url="https://feed.test/q", alerter=alerter,
                          retry_delays_s=[0, 0])
    assert out["status"] == "failed"
    assert last_attempt(db)["status"] == "failed"
    assert email.sent  # admin emailed
    assert email.sent[0]["subject"].startswith("[wswdy] mpd_down")


async def test_fetch_uses_fixture_when_path_provided(db, tmp_path):
    fixture = tmp_path / "mpd.json"
    fixture.write_bytes(FIXTURE.read_bytes())
    email = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="a@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_fetch(db=db, feed_url="https://unused.test", alerter=alerter,
                          fixture_path=str(fixture))
    assert out["status"] == "ok"
    assert db.execute("SELECT COUNT(*) FROM crimes").fetchone()[0] > 0
```

- [ ] **Step 2: Implement**

`src/wswdy/jobs/fetch.py`:
```python
"""Daily MPD GeoJSON fetch job."""
import asyncio
import json
import logging
import sqlite3
from pathlib import Path

from wswdy.alerts import AdminAlerter
from wswdy.clients.mpd import fetch_recent_geojson, parse_features
from wswdy.repos.crimes import upsert_many
from wswdy.repos.fetch_log import record_failure, record_success

log = logging.getLogger(__name__)

DEFAULT_RETRY_DELAYS_S = [300, 900, 2700]  # 5, 15, 45 minutes


async def run_fetch(
    *, db: sqlite3.Connection, feed_url: str, alerter: AdminAlerter,
    fixture_path: str | None = None,
    retry_delays_s: list[int] | None = None,
) -> dict:
    """Fetches the MPD feed (with retries) and upserts into the crimes table.

    Returns: {"status": "ok"|"failed", "added": int, "updated": int, "error"?: str}
    """
    if fixture_path:
        log.info("Using fixture instead of live feed: %s", fixture_path)
        try:
            data = json.loads(Path(fixture_path).read_text())
            features = parse_features(data)
            added, updated = upsert_many(db, features)
            record_success(db, added=added, updated=updated)
            return {"status": "ok", "added": added, "updated": updated}
        except Exception as e:
            record_failure(db, error=str(e))
            await alerter.alert(alert_type="mpd_down", message=f"fixture load failed: {e}")
            return {"status": "failed", "error": str(e)}

    delays = retry_delays_s if retry_delays_s is not None else DEFAULT_RETRY_DELAYS_S
    last_error: str | None = None
    for attempt, delay in enumerate([0, *delays]):
        if delay:
            log.info("Retrying MPD fetch in %ds (attempt %d)", delay, attempt + 1)
            await asyncio.sleep(delay)
        try:
            data = await fetch_recent_geojson(feed_url)
            features = parse_features(data)
            added, updated = upsert_many(db, features)
            record_success(db, added=added, updated=updated)
            log.info("MPD fetch ok: +%d / ~%d", added, updated)
            return {"status": "ok", "added": added, "updated": updated}
        except Exception as e:
            last_error = str(e)
            log.warning("MPD fetch attempt %d failed: %s", attempt + 1, e)

    record_failure(db, error=last_error or "unknown")
    await alerter.alert(
        alert_type="mpd_down",
        message=f"MPD feed unreachable after {len(delays) + 1} attempts: {last_error}",
    )
    return {"status": "failed", "error": last_error}
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_jobs_fetch.py -v
git add src/wswdy/jobs tests/test_jobs_fetch.py
git commit -m "feat(jobs): MPD fetch job with retries + admin alert on persistent failure"
```

---

### Task 22: Send job (`src/wswdy/jobs/send.py`)

**Files:**
- Create: `src/wswdy/jobs/send.py`, `tests/test_jobs_send.py`

- [ ] **Step 1: Write the failing test**

`tests/test_jobs_send.py`:
```python
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch
import pytest
from wswdy.jobs.send import run_daily_sends
from wswdy.notifiers.fake import FakeNotifier
from wswdy.alerts import AdminAlerter
from wswdy.repos.crimes import upsert_many
from wswdy.repos.subscribers import insert_pending, set_status
from wswdy.repos.send_log import exists_for_today


def _seed_subscriber(db, sid="s1", channel="email"):
    insert_pending(db, sid=sid, display_name="Jane",
                   email="jane@example.com" if channel == "email" else None,
                   phone="+12025551234" if channel == "whatsapp" else None,
                   preferred_channel=channel,
                   address_text="1500 14th St NW",
                   lat=38.9097, lon=-77.0319, radius_m=1000)
    set_status(db, sid, "APPROVED")


def _seed_crime(db, ccn="C1", offense="THEFT/OTHER",
                lat=38.9100, lon=-77.0319, when_iso=None):
    upsert_many(db, [{
        "ccn": ccn, "offense": offense, "method": None, "shift": "DAY",
        "block_address": "x", "lat": lat, "lon": lon,
        "report_dt": when_iso or "2026-04-27T12:00:00Z",
        "start_dt": None, "end_dt": None, "ward": "2", "district": "3",
        "raw_json": "{}",
    }])


async def test_send_daily_emails_active_subscriber(db, tmp_path):
    _seed_subscriber(db, "s1", channel="email")
    _seed_crime(db, when_iso="2026-04-27T15:00:00Z")
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    out = await run_daily_sends(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        send_date="2026-04-28", now_iso="2026-04-28T10:00:00+00:00",
        stagger=False, render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert out["sent"] == 1
    assert email.sent and "Jane" in email.sent[0]["text"]
    assert exists_for_today(db, "s1", "2026-04-28", "email")


async def test_send_skips_already_sent_today(db, tmp_path):
    _seed_subscriber(db, "s1", channel="email")
    _seed_crime(db)
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    args = dict(db=db, email=email, whatsapp=wa, alerter=alerter,
                base_url="https://x", hmac_secret="s",
                send_date="2026-04-28", now_iso="2026-04-28T10:00:00+00:00",
                stagger=False,
                render_static_map=AsyncMock(return_value=tmp_path / "p.png"))
    await run_daily_sends(**args)
    second = await run_daily_sends(**args)
    assert second["sent"] == 0
    assert second["skipped"] == 1
    assert len(email.sent) == 1


async def test_send_appends_mpd_warning_when_feed_stale(db, tmp_path):
    """If most recent fetch failed and last successful is >24h old, append warning."""
    from wswdy.repos.fetch_log import record_failure, record_success
    record_success(db, added=0, updated=0)  # initial
    # Simulate: last successful was 2 days ago, then a failure today
    db.execute("UPDATE fetch_log SET fetched_at='2026-04-26T05:30:00+00:00'")
    record_failure(db, error="503")
    db.commit()

    _seed_subscriber(db, "s1", channel="email")
    _seed_crime(db)
    email = FakeNotifier()
    wa = FakeNotifier()
    alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                           ha_webhook_url="", suppression_hours=6)
    await run_daily_sends(
        db=db, email=email, whatsapp=wa, alerter=alerter,
        base_url="https://x", hmac_secret="s",
        send_date="2026-04-28", now_iso="2026-04-28T10:00:00+00:00",
        stagger=False, render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
    )
    assert "MPD" in email.sent[0]["text"] or "delayed" in email.sent[0]["text"].lower()


async def test_send_logs_failure_and_continues(db, tmp_path):
    _seed_subscriber(db, "ok", channel="email")
    _seed_subscriber(db, "fail", channel="email")
    _seed_crime(db)

    email = FakeNotifier()  # ok for "ok"
    failing = FakeNotifier(fail_with="smtp 530")

    # Patch dispatch so the second subscriber sees the failing notifier
    async def patched_dispatch(sub, **kw):
        if sub["id"] == "fail":
            return await failing.send(recipient=sub["email"], subject=kw["subject"],
                                      text=kw["text"], image_path=kw["image_path"])
        return await email.send(recipient=sub["email"], subject=kw["subject"],
                                text=kw["text"], image_path=kw["image_path"])

    with patch("wswdy.jobs.send.dispatch", new=patched_dispatch):
        alerter = AdminAlerter(db=db, email=email, admin_email="admin@x",
                               ha_webhook_url="", suppression_hours=6)
        out = await run_daily_sends(
            db=db, email=email, whatsapp=failing, alerter=alerter,
            base_url="https://x", hmac_secret="s",
            send_date="2026-04-28", now_iso="2026-04-28T10:00:00+00:00",
            stagger=False, render_static_map=AsyncMock(return_value=tmp_path / "p.png"),
        )
    assert out["sent"] == 1
    assert out["failed"] == 1
```

- [ ] **Step 2: Implement**

`src/wswdy/jobs/send.py`:
```python
"""Daily digest send job (staggered)."""
import asyncio
import logging
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable

from wswdy.alerts import AdminAlerter
from wswdy.digest import build_digest_text
from wswdy.notifiers.base import Notifier, dispatch
from wswdy.repos.crimes import list_in_radius_window
from wswdy.repos.fetch_log import last_successful, last_attempt
from wswdy.repos.send_log import exists_for_today, record
from wswdy.repos.subscribers import list_active, set_last_sent
from wswdy.tokens import sign

log = logging.getLogger(__name__)


async def run_daily_sends(
    *, db: sqlite3.Connection,
    email: Notifier, whatsapp: Notifier, alerter: AdminAlerter,
    base_url: str, hmac_secret: str,
    send_date: str, now_iso: str,
    stagger: bool = True,
    stagger_max_s: int = 45 * 60, gap_min_s: int = 30, gap_max_s: int = 120,
    render_static_map: Callable[..., Awaitable[Path]] | None = None,
    static_map_dir: Path = Path("./static_maps"),
) -> dict:
    actives = list_active(db)
    log.info("Daily send: %d active subscribers", len(actives))

    mpd_warning = _is_feed_stale(db, now_iso=now_iso)

    sent = failed = skipped = 0
    end_iso = now_iso
    start_dt = datetime.fromisoformat(now_iso) - timedelta(hours=24)
    start_iso = start_dt.isoformat(timespec="seconds")

    if stagger:
        random.shuffle(actives)

    for i, sub in enumerate(actives):
        if exists_for_today(db, sub["id"], send_date, sub["preferred_channel"]):
            skipped += 1
            continue

        if stagger and i > 0:
            await asyncio.sleep(random.uniform(gap_min_s, gap_max_s))

        crimes = list_in_radius_window(
            db, sub["lat"], sub["lon"], sub["radius_m"],
            start=start_iso, end=end_iso,
        )

        map_token = sign(hmac_secret, purpose="map", subscriber_id=sub["id"])
        unsub_token = sign(hmac_secret, purpose="unsubscribe", subscriber_id=sub["id"])
        map_url = f"{base_url}/map/{sub['id']}?token={map_token}"
        unsub_url = f"{base_url}/u/{sub['id']}?token={unsub_token}"

        text = build_digest_text(
            display_name=sub["display_name"], radius_m=sub["radius_m"],
            crimes=crimes, home_lat=sub["lat"], home_lon=sub["lon"],
            map_url=map_url, unsubscribe_url=unsub_url,
            mpd_warning=mpd_warning,
        )

        # Static map preview (best-effort; we still send text-only on render failure)
        image_path: Path | None = None
        if render_static_map is not None:
            try:
                image_path = await render_static_map(
                    center_lat=sub["lat"], center_lon=sub["lon"],
                    radius_m=sub["radius_m"],
                    markers=[(c["lat"], c["lon"], _tier_for(c)) for c in crimes],
                    out_path=static_map_dir / f"{sub['id']}_{send_date}.png",
                )
            except Exception as e:
                log.warning("Static map render failed for %s: %s", sub["id"], e)

        result = await dispatch(
            sub, email_notifier=email, whatsapp_notifier=whatsapp,
            subject=f"DC briefing for {sub['display_name']} — {send_date}",
            text=text, image_path=image_path,
        )

        if result.ok:
            record(db, sub["id"], send_date, sub["preferred_channel"], "sent")
            set_last_sent(db, sub["id"], now_iso)
            sent += 1
        else:
            record(db, sub["id"], send_date, sub["preferred_channel"], "failed",
                   error=f"{result.error}: {result.detail or ''}")
            failed += 1
            if result.error == "session_expired":
                await alerter.alert(
                    alert_type="whatsapp_session_expired",
                    message="WhatsApp MCP session expired — re-link the device.",
                )

    log.info("Daily send done: sent=%d failed=%d skipped=%d", sent, failed, skipped)
    return {"sent": sent, "failed": failed, "skipped": skipped}


def _tier_for(crime: dict) -> int:
    from wswdy.tiers import classify
    return classify(crime["offense"], crime.get("method"))


def _is_feed_stale(db: sqlite3.Connection, *, now_iso: str) -> bool:
    """True if the last successful fetch was >24h before now_iso."""
    last_ok = last_successful(db)
    if not last_ok:
        return True
    last_dt = datetime.fromisoformat(last_ok["fetched_at"].replace("Z", "+00:00"))
    now_dt = datetime.fromisoformat(now_iso)
    return (now_dt - last_dt) > timedelta(hours=24)
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_jobs_send.py -v
git add src/wswdy/jobs/send.py tests/test_jobs_send.py
git commit -m "feat(jobs): daily digest send job with staggering + idempotency"
```

---

### Task 23: Prune + health snapshot jobs (`src/wswdy/jobs/prune.py`, `health.py`)

**Files:**
- Create: `src/wswdy/jobs/prune.py`, `src/wswdy/jobs/health.py`, `tests/test_jobs_prune.py`, `tests/test_jobs_health.py`

- [ ] **Step 1: Write tests**

`tests/test_jobs_prune.py`:
```python
from wswdy.jobs.prune import run_prune
from wswdy.repos.crimes import upsert_many


def test_run_prune_deletes_crimes_older_than_90_days(db):
    upsert_many(db, [
        {"ccn": "old", "offense": "THEFT/OTHER", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9, "lon": -77.0,
         "report_dt": "2025-01-01T00:00:00Z",
         "start_dt": None, "end_dt": None, "ward": None, "district": None,
         "raw_json": "{}"},
        {"ccn": "new", "offense": "THEFT/OTHER", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9, "lon": -77.0,
         "report_dt": "2026-04-27T00:00:00Z",
         "start_dt": None, "end_dt": None, "ward": None, "district": None,
         "raw_json": "{}"},
    ])
    deleted = run_prune(db, today_iso="2026-04-28T00:00:00+00:00", days=90)
    assert deleted == 1
    rows = db.execute("SELECT ccn FROM crimes").fetchall()
    assert [r["ccn"] for r in rows] == ["new"]
```

`tests/test_jobs_health.py`:
```python
from datetime import datetime, timezone
from wswdy.jobs.health import run_health_snapshot
from wswdy.notifiers.fake import FakeNotifier
from wswdy.repos.subscribers import insert_pending, set_status
from wswdy.repos.send_log import record
from wswdy.repos.fetch_log import record_success


async def test_health_snapshot_emails_admin(db):
    insert_pending(db, sid="a", display_name="A", email="a@x", phone=None,
                   preferred_channel="email", address_text="x",
                   lat=38.9, lon=-77.0, radius_m=1000)
    set_status(db, "a", "APPROVED")
    record_success(db, added=42, updated=3)
    record(db, "a", "2026-04-28", "email", "sent")

    email = FakeNotifier()
    out = await run_health_snapshot(db=db, email=email, admin_email="admin@x",
                                    today="2026-04-28")
    assert out["sent"] == 1
    assert email.sent[0]["recipient"] == "admin@x"
    body = email.sent[0]["text"]
    assert "fetched 42" in body or "+42" in body
    assert "1 sent" in body or "sent: 1" in body.lower()
```

- [ ] **Step 2: Implement prune**

`src/wswdy/jobs/prune.py`:
```python
"""Prune crimes older than N days."""
import sqlite3
from datetime import datetime, timedelta

from wswdy.repos.crimes import prune_older_than


def run_prune(db: sqlite3.Connection, *, today_iso: str, days: int = 90) -> int:
    cutoff = datetime.fromisoformat(today_iso) - timedelta(days=days)
    return prune_older_than(db, cutoff.isoformat(timespec="seconds"))
```

- [ ] **Step 3: Implement health snapshot**

`src/wswdy/jobs/health.py`:
```python
"""Daily 23:00 ET health snapshot email to the admin."""
import sqlite3

from wswdy.notifiers.base import Notifier
from wswdy.repos.fetch_log import last_attempt
from wswdy.repos.send_log import send_volume_last_n_days, recent_failures
from wswdy.repos.subscribers import list_by_status


async def run_health_snapshot(*, db: sqlite3.Connection, email: Notifier,
                              admin_email: str, today: str) -> dict:
    pending = len(list_by_status(db, "PENDING"))
    approved = len(list_by_status(db, "APPROVED"))
    unsub = len(list_by_status(db, "UNSUBSCRIBED"))
    last_fetch = last_attempt(db) or {}
    today_volume = [r for r in send_volume_last_n_days(db, n=1, today=today)
                    if r["send_date"] == today]
    sent = today_volume[0]["sent"] if today_volume else 0
    failed = today_volume[0]["failed"] if today_volume else 0
    fails = recent_failures(db, limit=5)

    lines = [
        f"wswdy daily health — {today}",
        "",
        f"Subscribers: {approved} approved · {pending} pending · {unsub} unsubscribed",
        f"MPD fetch:   {last_fetch.get('status', 'never')} (+{last_fetch.get('crimes_added') or 0}, "
        f"~{last_fetch.get('crimes_updated') or 0}) at {last_fetch.get('fetched_at', 'n/a')}",
        f"Sends today: {sent} sent · {failed} failed",
    ]
    if fails:
        lines.append("")
        lines.append("Recent failures:")
        for f in fails:
            lines.append(f"  · {f['subscriber_id']} ({f['channel']}): {f['error']}")

    text = "\n".join(lines)
    res = await email.send(recipient=admin_email,
                           subject=f"[wswdy] daily snapshot {today}",
                           text=text, image_path=None)
    return {"sent": 1 if res.ok else 0, "error": res.error}
```

- [ ] **Step 4: Run; commit**

```bash
pytest tests/test_jobs_prune.py tests/test_jobs_health.py -v
git add src/wswdy/jobs/prune.py src/wswdy/jobs/health.py tests/test_jobs_prune.py tests/test_jobs_health.py
git commit -m "feat(jobs): prune + daily health snapshot"
```

**End of Phase 5.** All scheduled work is implemented and tested.

---

## Phase 6 — FastAPI app + routes + templates (Tasks 24–31)

### Task 24: FastAPI app skeleton + base template + healthz (`src/wswdy/main.py`, base template)

**Files:**
- Create: `src/wswdy/main.py`, `src/wswdy/routes/__init__.py`, `src/wswdy/routes/health.py`, `src/wswdy/templates/base.html`, `src/wswdy/static/shared.css`, `tests/test_routes_health.py`

- [ ] **Step 1: Port shared.css from mockups**

```bash
mkdir -p src/wswdy/static src/wswdy/templates/email src/wswdy/templates/whatsapp src/wswdy/routes
echo '"""HTTP routes."""' > src/wswdy/routes/__init__.py
cp mockups/shared.css src/wswdy/static/shared.css
```

- [ ] **Step 2: Create base template**

`src/wswdy/templates/base.html`:
```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{% block title %}wswdy — DC Crime Alerts{% endblock %}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wdth,wght@12..96,75..100,200..800&family=Geist:wght@300..700&family=Geist+Mono:wght@400;500&display=swap" rel="stylesheet" />
  <link rel="stylesheet" href="/static/shared.css" />
  {% block head_extra %}{% endblock %}
</head>
<body>
  <header class="nav">
    <a href="/" class="nav-mark">/wswdy</a>
    <div class="nav-meta">{% block nav_meta %}DC · friends &amp; family alpha{% endblock %}</div>
  </header>
  {% block content %}{% endblock %}
  {% block scripts %}{% endblock %}
</body>
</html>
```

- [ ] **Step 3: Write the healthz test**

`tests/test_routes_health.py`:
```python
from fastapi.testclient import TestClient
from wswdy.main import create_app


def test_healthz_ok(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "x")
    monkeypatch.setenv("ADMIN_TOKEN", "y")
    monkeypatch.setenv("MAPTILER_API_KEY", "z")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    app = create_app()
    client = TestClient(app)
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_static_css_served(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "x")
    monkeypatch.setenv("ADMIN_TOKEN", "y")
    monkeypatch.setenv("MAPTILER_API_KEY", "z")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    app = create_app()
    client = TestClient(app)
    r = client.get("/static/shared.css")
    assert r.status_code == 200
    assert b"--paper" in r.content or b"--bg" in r.content
```

- [ ] **Step 4: Implement health route**

`src/wswdy/routes/health.py`:
```python
from fastapi import APIRouter

router = APIRouter()


@router.get("/healthz")
def healthz():
    return {"status": "ok"}
```

- [ ] **Step 5: Implement app factory**

`src/wswdy/main.py`:
```python
"""FastAPI application factory."""
import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from wswdy.config import get_settings
from wswdy.db import connect, init_schema
from wswdy.routes import health


PKG_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PKG_DIR / "templates"
STATIC_DIR = PKG_DIR / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def create_app() -> FastAPI:
    settings = get_settings()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    app = FastAPI(title="wswdy", version="0.1.0")
    app.state.settings = settings

    # DB connection — single shared connection, WAL mode tolerates concurrent readers
    app.state.db = connect(settings.db_path)
    init_schema(app.state.db)

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(health.router)

    return app


app = create_app()
```

- [ ] **Step 6: Run; commit**

```bash
pytest tests/test_routes_health.py -v
git add src/wswdy/main.py src/wswdy/routes/__init__.py src/wswdy/routes/health.py \
        src/wswdy/templates/base.html src/wswdy/static/shared.css \
        tests/test_routes_health.py
git commit -m "feat(app): FastAPI app factory + base template + healthz"
```

---

### Task 25: Rate limiter (`src/wswdy/ratelimit.py`)

**Files:**
- Create: `src/wswdy/ratelimit.py`, `tests/test_ratelimit.py`

- [ ] **Step 1: Write the failing test**

`tests/test_ratelimit.py`:
```python
import time
import pytest
from wswdy.ratelimit import RateLimiter


def test_under_limit_allowed():
    rl = RateLimiter(max_requests=3, window_s=60)
    assert rl.check("1.2.3.4") is True
    assert rl.check("1.2.3.4") is True
    assert rl.check("1.2.3.4") is True


def test_over_limit_rejected():
    rl = RateLimiter(max_requests=2, window_s=60)
    rl.check("1.2.3.4")
    rl.check("1.2.3.4")
    assert rl.check("1.2.3.4") is False


def test_window_resets():
    rl = RateLimiter(max_requests=1, window_s=1)
    assert rl.check("a") is True
    assert rl.check("a") is False
    time.sleep(1.1)
    assert rl.check("a") is True


def test_per_ip_isolation():
    rl = RateLimiter(max_requests=1, window_s=60)
    assert rl.check("a") is True
    assert rl.check("a") is False
    assert rl.check("b") is True
```

- [ ] **Step 2: Implement**

`src/wswdy/ratelimit.py`:
```python
"""Simple in-memory IP-based rate limiter (sliding window)."""
import time
from collections import defaultdict, deque


class RateLimiter:
    def __init__(self, *, max_requests: int, window_s: int):
        self.max = max_requests
        self.window = window_s
        self._buckets: dict[str, deque[float]] = defaultdict(deque)

    def check(self, key: str) -> bool:
        now = time.monotonic()
        bucket = self._buckets[key]
        cutoff = now - self.window
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= self.max:
            return False
        bucket.append(now)
        return True
```

- [ ] **Step 3: Run; commit**

```bash
pytest tests/test_ratelimit.py -v
git add src/wswdy/ratelimit.py tests/test_ratelimit.py
git commit -m "feat(ratelimit): in-memory sliding-window IP rate limiter"
```

---

### Task 26: Signup form + POST /signup (`src/wswdy/routes/public.py`, `signup.html`)

**Files:**
- Create: `src/wswdy/routes/public.py`, `src/wswdy/templates/signup.html`, `src/wswdy/templates/email/admin_review.html`, `tests/test_routes_public.py`
- Modify: `src/wswdy/main.py` (mount router)

- [ ] **Step 1: Port the signup template from the mockup**

Copy `mockups/index.html` to `src/wswdy/templates/signup.html`. Then convert it to extend `base.html`:

`src/wswdy/templates/signup.html`:
```html
{% extends "base.html" %}
{% block title %}What shit went down yesterday?{% endblock %}
{% block content %}
<main class="page fade-in">
  <section class="hero">
    <div class="hero-eyebrow">A daily 6am text · MPD-sourced</div>
    <h1 class="display hero-title">
      What <span class="accent">shit</span> went down yesterday?
    </h1>
    <p class="hero-sub">
      A quiet morning summary of crimes reported within walking distance of your home in DC.
      Email or WhatsApp. Free. No ads. Run by a neighbor.
    </p>
  </section>

  {% if error %}<div class="alert alert-error">{{ error }}</div>{% endif %}

  <form id="signup" class="card" method="post" action="/signup">
    <h2 class="card-title">Get the briefing</h2>
    <p class="card-sub">Two minutes. We'll review and confirm by email.</p>

    <div class="field">
      <label class="field-label" for="name">First name</label>
      <input id="name" name="display_name" class="field-input" type="text"
             placeholder="Jane" autocomplete="given-name" required />
    </div>

    <div class="field ac-wrap">
      <label class="field-label" for="addr">Home address</label>
      <input id="addr" name="address_text" class="field-input" type="text"
             autocomplete="off" required />
      <input type="hidden" name="lat" id="lat" />
      <input type="hidden" name="lon" id="lon" />
      <ul class="ac-list" id="ac-list" hidden></ul>
    </div>

    <div class="field">
      <label class="field-label">How should we reach you?</label>
      <div class="seg" id="seg">
        <input type="radio" name="preferred_channel" id="ch-email" value="email" checked />
        <label for="ch-email">Email</label>
        <input type="radio" name="preferred_channel" id="ch-wa" value="whatsapp" />
        <label for="ch-wa">WhatsApp</label>
        <span class="seg-thumb"></span>
      </div>
    </div>

    <div class="field" id="email-field">
      <label class="field-label" for="email">Email address</label>
      <input id="email" name="email" class="field-input" type="email"
             placeholder="jane@example.com" />
    </div>
    <div class="field" id="phone-field" hidden>
      <label class="field-label" for="phone">Phone (E.164)</label>
      <input id="phone" name="phone" class="field-input" type="tel"
             placeholder="+1 202 555 0123" />
    </div>

    <div class="field">
      <div class="radius-row">
        <label class="field-label" style="margin: 0;">How far should we look?</label>
        <div class="radius-value">
          <span id="radius-num">1,000</span><span class="unit">m</span>
        </div>
      </div>
      <div class="slider">
        <div class="slider-track"><div class="slider-fill" id="slider-fill" style="width: 44%;"></div></div>
        <input type="range" name="radius_m" min="200" max="2000" step="100" value="1000" id="radius" />
        <div class="slider-thumb" id="slider-thumb" style="left: 44%;"></div>
      </div>
      <div class="slider-labels"><span>200m</span><span>1,000m</span><span>2,000m</span></div>
    </div>

    <div class="preview empty" id="preview">
      <div class="preview-eyebrow">
        <span class="l">Within <span id="prev-r" class="tnum">1,000</span>m · last 7 days</span>
        <span class="r">live</span>
      </div>
      <div class="preview-headline">
        <span class="preview-number" id="prev-total">—</span>
        <span class="preview-label">crimes reported</span>
      </div>
      <div class="preview-sub">≈ <span class="tnum" id="prev-avg">—</span> per day on average</div>
      <div class="preview-bar" aria-hidden="true">
        <span style="background: var(--t1);"></span>
        <span style="background: var(--t2);"></span>
        <span style="background: var(--t3);"></span>
        <span style="background: var(--t4);"></span>
      </div>
      <div class="preview-legend">
        <div class="preview-legend-item"><span class="tier-dot t1"></span><span class="lbl">Violent</span><span class="num">—</span></div>
        <div class="preview-legend-item"><span class="tier-dot t2"></span><span class="lbl">Serious property</span><span class="num">—</span></div>
        <div class="preview-legend-item"><span class="tier-dot t3"></span><span class="lbl">Vehicle</span><span class="num">—</span></div>
        <div class="preview-legend-item"><span class="tier-dot t4"></span><span class="lbl">Petty</span><span class="num">—</span></div>
      </div>
      <div class="preview-empty-msg">
        Enter an address above to preview crime activity in your area.
      </div>
    </div>

    <div class="submit-area">
      <button type="submit" class="btn btn-primary btn-block btn-lg">
        Request access &nbsp;→
      </button>
      <p class="submit-fineprint">
        We'll email you to confirm. Daily messages start the morning after we approve.
        Unsubscribe anytime — one click, no questions.
      </p>
    </div>
  </form>

  <footer class="footer">
    <div>data: <a href="https://opendata.dc.gov/datasets/" class="tlink">DC MPD open feed</a></div>
    <div>dccrime.iandmuir.com</div>
  </footer>
</main>
{% endblock %}
{% block scripts %}
<script>
  // Channel toggle
  const seg = document.getElementById('seg');
  const emailField = document.getElementById('email-field');
  const phoneField = document.getElementById('phone-field');
  document.querySelectorAll('input[name=preferred_channel]').forEach(r => {
    r.addEventListener('change', () => {
      const isWA = r.value === 'whatsapp' && r.checked;
      seg.classList.toggle('is-whatsapp', isWA);
      emailField.hidden = isWA;
      phoneField.hidden = !isWA;
    });
  });

  // Radius slider
  const r = document.getElementById('radius');
  const num = document.getElementById('radius-num');
  const fill = document.getElementById('slider-fill');
  const thumb = document.getElementById('slider-thumb');
  const prevR = document.getElementById('prev-r');
  let debounceTimer;

  function updateSlider() {
    const v = +r.value;
    const pct = ((v - 200) / 1800) * 100;
    num.textContent = v.toLocaleString('en-US');
    prevR.textContent = v.toLocaleString('en-US');
    fill.style.width = pct + '%';
    thumb.style.left = pct + '%';
    schedulePreview();
  }
  r.addEventListener('input', updateSlider);
  updateSlider();

  // Address autocomplete via MapTiler — fetched client-side via /api/geocode? No,
  // we just call MapTiler directly with a public key restricted to our domain.
  // For now, do a simple debounced fetch and dropdown render.
  const addrInput = document.getElementById('addr');
  const acList = document.getElementById('ac-list');
  const latInput = document.getElementById('lat');
  const lonInput = document.getElementById('lon');
  let acTimer;
  addrInput.addEventListener('input', () => {
    clearTimeout(acTimer);
    acTimer = setTimeout(async () => {
      const q = addrInput.value.trim();
      if (q.length < 3) { acList.hidden = true; return; }
      const r = await fetch(`/api/geocode?q=${encodeURIComponent(q)}`);
      if (!r.ok) return;
      const data = await r.json();
      acList.innerHTML = data.results.map(item =>
        `<li data-lat="${item.lat}" data-lon="${item.lon}"><span class="pin"></span>
         <span class="ac-main"><strong>${item.display}</strong></span></li>`).join('');
      acList.hidden = data.results.length === 0;
      acList.querySelectorAll('li').forEach(li => {
        li.addEventListener('click', () => {
          addrInput.value = li.querySelector('strong').textContent;
          latInput.value = li.dataset.lat;
          lonInput.value = li.dataset.lon;
          acList.hidden = true;
          schedulePreview();
        });
      });
    }, 300);
  });

  // Live preview
  function schedulePreview() {
    clearTimeout(debounceTimer);
    if (!latInput.value || !lonInput.value) return;
    const preview = document.getElementById('preview');
    preview.classList.remove('empty', 'zero');
    preview.classList.add('loading');
    debounceTimer = setTimeout(async () => {
      const r = await fetch('/api/preview', {
        method: 'POST', headers: {'content-type': 'application/json'},
        body: JSON.stringify({lat: +latInput.value, lon: +lonInput.value,
                              radius_m: +document.getElementById('radius').value}),
      });
      preview.classList.remove('loading');
      if (!r.ok) { preview.classList.add('empty'); return; }
      const d = await r.json();
      const total = d.total;
      document.getElementById('prev-total').textContent = total;
      document.getElementById('prev-avg').textContent = d.avg_per_day.toFixed(1);
      const sum = total || 1;
      const bars = document.querySelectorAll('.preview-bar > span');
      bars[0].style.width = (d.by_tier['1'] / sum * 100) + '%';
      bars[1].style.width = (d.by_tier['2'] / sum * 100) + '%';
      bars[2].style.width = (d.by_tier['3'] / sum * 100) + '%';
      bars[3].style.width = (d.by_tier['4'] / sum * 100) + '%';
      const nums = document.querySelectorAll('.preview-legend-item .num');
      nums[0].textContent = d.by_tier['1'];
      nums[1].textContent = d.by_tier['2'];
      nums[2].textContent = d.by_tier['3'];
      nums[3].textContent = d.by_tier['4'];
      if (total === 0) preview.classList.add('zero');
    }, 300);
  }
</script>
{% endblock %}
```

- [ ] **Step 2: Write the failing route test**

`tests/test_routes_public.py`:
```python
from unittest.mock import AsyncMock, patch
import pytest
from fastapi.testclient import TestClient
from wswdy.main import create_app
from wswdy.repos.subscribers import list_by_status


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setenv("WSWDY_BASE_URL", "https://x.test")
    monkeypatch.setenv("ADMIN_EMAIL", "admin@x")
    return create_app()


def test_get_signup_form_renders(app):
    client = TestClient(app)
    r = client.get("/")
    assert r.status_code == 200
    assert b"What" in r.content and b"shit" in r.content
    assert b"display_name" in r.content


@patch("wswdy.routes.public.geocode_address", new_callable=AsyncMock)
def test_post_signup_creates_pending_and_emails_admin(mock_geo, app):
    mock_geo.return_value = {"lat": 38.9097, "lon": -77.0319,
                              "display": "1500 14th St NW, Washington, DC"}
    # Replace the email notifier with a FakeNotifier we can inspect
    from wswdy.notifiers.fake import FakeNotifier
    fake = FakeNotifier()
    app.state.email_notifier = fake

    client = TestClient(app)
    r = client.post("/signup", data={
        "display_name": "Jane",
        "address_text": "1500 14th St NW",
        "lat": "38.9097", "lon": "-77.0319",
        "preferred_channel": "email",
        "email": "jane@example.com",
        "radius_m": "1000",
    }, follow_redirects=False)
    assert r.status_code in (303, 302)

    pending = list_by_status(app.state.db, "PENDING")
    assert len(pending) == 1
    assert pending[0]["display_name"] == "Jane"
    assert fake.sent and fake.sent[0]["recipient"] == "admin@x"
    assert "Approve" in fake.sent[0]["text"]


@patch("wswdy.routes.public.geocode_address", new_callable=AsyncMock)
def test_post_signup_outside_dc_returns_form_with_error(mock_geo, app):
    from wswdy.clients.maptiler import GeocodeError
    mock_geo.side_effect = GeocodeError("address is outside DC")
    client = TestClient(app)
    r = client.post("/signup", data={
        "display_name": "Bob",
        "address_text": "1 Inner Harbor, Baltimore",
        "preferred_channel": "email",
        "email": "bob@x",
        "radius_m": "1000",
    })
    assert r.status_code == 400
    assert b"outside DC" in r.content


def test_post_signup_rate_limited(app):
    client = TestClient(app)
    for _ in range(10):
        client.post("/signup", data={"display_name": "x", "address_text": "y",
                                      "preferred_channel": "email", "email": "x@x",
                                      "radius_m": "1000"})
    r = client.post("/signup", data={"display_name": "x", "address_text": "y",
                                      "preferred_channel": "email", "email": "x@x",
                                      "radius_m": "1000"})
    assert r.status_code == 429
```

- [ ] **Step 3: Implement the public routes**

`src/wswdy/routes/public.py`:
```python
"""Signup form + POST /signup + JSON helpers used by the form."""
from fastapi import APIRouter, Form, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from wswdy.clients.maptiler import geocode_address, GeocodeError
from wswdy.ids import new_subscriber_id
from wswdy.notifiers.base import Notifier
from wswdy.ratelimit import RateLimiter
from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import sign

router = APIRouter()
_signup_rl = RateLimiter(max_requests=10, window_s=3600)
_geocode_rl = RateLimiter(max_requests=60, window_s=60)


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _email_notifier(request: Request) -> Notifier:
    """The app exposes an email notifier on app.state for tests to override."""
    return request.app.state.email_notifier


@router.get("/", response_class=HTMLResponse)
async def signup_form(request: Request):
    from wswdy.main import templates
    return templates.TemplateResponse(request, "signup.html", {"error": None})


@router.post("/signup")
async def signup_submit(
    request: Request,
    display_name: str = Form(...),
    address_text: str = Form(...),
    preferred_channel: str = Form(...),
    radius_m: int = Form(...),
    email: str = Form(""),
    phone: str = Form(""),
    lat: float | None = Form(None),
    lon: float | None = Form(None),
):
    if not _signup_rl.check(_client_ip(request)):
        return Response(status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        content="Too many signup attempts. Try again in an hour.")

    settings = request.app.state.settings
    from wswdy.main import templates

    # Geocode (or trust hidden lat/lon if both present and in DC)
    try:
        if lat is not None and lon is not None:
            from wswdy.geo import in_dc_bbox
            if not in_dc_bbox(lat, lon):
                raise GeocodeError("address is outside DC")
            place = {"lat": lat, "lon": lon, "display": address_text}
        else:
            place = await geocode_address(address_text, api_key=settings.maptiler_api_key)
    except GeocodeError as e:
        return templates.TemplateResponse(
            request, "signup.html", {"error": str(e)},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    if preferred_channel not in {"email", "whatsapp"}:
        return Response(status_code=400, content="invalid channel")
    if preferred_channel == "email" and not email:
        return Response(status_code=400, content="email required")
    if preferred_channel == "whatsapp" and not phone:
        return Response(status_code=400, content="phone required")
    if not (200 <= radius_m <= 2000):
        return Response(status_code=400, content="radius out of range")

    sid = new_subscriber_id()
    subs_repo.insert_pending(
        request.app.state.db, sid=sid, display_name=display_name,
        email=email or None, phone=phone or None,
        preferred_channel=preferred_channel, address_text=address_text,
        lat=place["lat"], lon=place["lon"], radius_m=radius_m,
    )

    # Email admin a review link
    token = sign(settings.hmac_secret, purpose="approve",
                 subscriber_id=sid, ttl_seconds=7 * 86400)
    review_url = f"{settings.base_url}/a/{token}"
    body = (
        f"New wswdy signup from {display_name}.\n\n"
        f"Channel: {preferred_channel} ({email or phone})\n"
        f"Address: {place['display']}\n"
        f"Coords:  {place['lat']:.4f}, {place['lon']:.4f}\n"
        f"Radius:  {radius_m}m\n\n"
        f"Approve or reject:\n{review_url}\n"
    )
    await _email_notifier(request).send(
        recipient=settings.admin_email,
        subject=f"[wswdy] new signup: {display_name}",
        text=body, image_path=None,
    )

    return RedirectResponse(url="/?submitted=1", status_code=303)


@router.get("/api/geocode", response_class=JSONResponse)
async def geocode_endpoint(request: Request, q: str):
    if not _geocode_rl.check(_client_ip(request)):
        return JSONResponse({"results": []}, status_code=429)
    settings = request.app.state.settings
    try:
        place = await geocode_address(q, api_key=settings.maptiler_api_key)
        return {"results": [{"lat": place["lat"], "lon": place["lon"],
                              "display": place["display"]}]}
    except GeocodeError:
        return {"results": []}
```

- [ ] **Step 4: Wire routes into `main.py`**

Edit `src/wswdy/main.py` to mount the new routes and create the email notifier:

```python
# Add imports near the top:
from wswdy.notifiers.email import EmailNotifier
from wswdy.notifiers.whatsapp import WhatsAppMcpNotifier
from wswdy.alerts import AdminAlerter
from wswdy.routes import public

# Inside create_app(), after include_router(health.router):
    app.state.email_notifier = EmailNotifier(
        host=settings.smtp_host, port=settings.smtp_port,
        user=settings.smtp_user, password=settings.smtp_pass,
        sender=settings.smtp_from,
    )
    app.state.whatsapp_notifier = WhatsAppMcpNotifier(
        base_url=settings.whatsapp_mcp_url, token=settings.whatsapp_mcp_token,
    )
    app.state.alerter = AdminAlerter(
        db=app.state.db, email=app.state.email_notifier,
        admin_email=settings.admin_email, ha_webhook_url=settings.ha_webhook_url,
    )

    app.include_router(public.router)
```

- [ ] **Step 5: Run; commit**

```bash
pytest tests/test_routes_public.py -v
git add src/wswdy/routes/public.py src/wswdy/templates/signup.html src/wswdy/main.py \
        tests/test_routes_public.py
git commit -m "feat(routes): signup form + POST /signup + admin review email"
```

---

### Task 27: Admin review routes (`src/wswdy/routes/admin_review.py`)

**Files:**
- Create: `src/wswdy/routes/admin_review.py`, `src/wswdy/templates/admin_review.html`, `src/wswdy/templates/whatsapp/welcome.txt`, `tests/test_routes_admin_review.py`
- Modify: `src/wswdy/main.py` (mount router)

- [ ] **Step 1: Write the failing test**

`tests/test_routes_admin_review.py`:
```python
import pytest
from fastapi.testclient import TestClient
from wswdy.main import create_app
from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import sign


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setenv("WSWDY_BASE_URL", "https://x.test")
    return create_app()


def _seed(app):
    subs_repo.insert_pending(
        app.state.db, sid="abc", display_name="Jane",
        email="jane@x", phone=None, preferred_channel="email",
        address_text="1 St", lat=38.9, lon=-77.0, radius_m=1000,
    )
    return sign("secret", purpose="approve", subscriber_id="abc", ttl_seconds=86400)


def test_get_review_renders_subscriber_summary(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/a/{token}")
    assert r.status_code == 200
    assert b"Jane" in r.content
    assert b"approve" in r.content.lower()


def test_post_approve_changes_status_and_sends_welcome(app):
    from wswdy.notifiers.fake import FakeNotifier
    fake = FakeNotifier()
    app.state.email_notifier = fake

    token = _seed(app)
    client = TestClient(app)
    r = client.post(f"/a/{token}/approve", follow_redirects=False)
    assert r.status_code in (200, 303)
    s = subs_repo.get(app.state.db, "abc")
    assert s["status"] == "APPROVED"
    # Welcome message went out
    assert any("welcome" in e["subject"].lower() or "confirmed" in e["text"].lower()
               for e in fake.sent)


def test_post_reject_changes_status(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.post(f"/a/{token}/reject")
    assert r.status_code in (200, 303)
    s = subs_repo.get(app.state.db, "abc")
    assert s["status"] == "REJECTED"


def test_invalid_token_rejected(app):
    client = TestClient(app)
    r = client.get("/a/not.a.real.token")
    assert r.status_code == 400


def test_expired_token_rejected(app):
    subs_repo.insert_pending(
        app.state.db, sid="abc", display_name="J", email="j@x", phone=None,
        preferred_channel="email", address_text="x", lat=38.9, lon=-77.0, radius_m=1000,
    )
    token = sign("secret", purpose="approve", subscriber_id="abc", ttl_seconds=-1)
    client = TestClient(app)
    r = client.get(f"/a/{token}")
    assert r.status_code == 400
```

- [ ] **Step 2: Implement the route**

`src/wswdy/routes/admin_review.py`:
```python
from fastapi import APIRouter, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from wswdy.notifiers.base import dispatch
from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import sign, verify, TokenError

router = APIRouter()


def _verify_or_400(request: Request, token: str) -> str | Response:
    secret = request.app.state.settings.hmac_secret
    try:
        payload = verify(secret, token, purpose="approve")
        return payload["subscriber_id"]
    except TokenError as e:
        return Response(status_code=400, content=f"invalid token: {e}")


@router.get("/a/{token}", response_class=HTMLResponse)
async def review_landing(request: Request, token: str):
    sid_or_resp = _verify_or_400(request, token)
    if isinstance(sid_or_resp, Response):
        return sid_or_resp
    sub = subs_repo.get(request.app.state.db, sid_or_resp)
    if not sub:
        return Response(status_code=404, content="subscriber not found")
    from wswdy.main import templates
    return templates.TemplateResponse(
        request, "admin_review.html", {"sub": sub, "token": token},
    )


@router.post("/a/{token}/approve")
async def review_approve(request: Request, token: str):
    sid_or_resp = _verify_or_400(request, token)
    if isinstance(sid_or_resp, Response):
        return sid_or_resp
    db = request.app.state.db
    sub = subs_repo.get(db, sid_or_resp)
    if not sub:
        return Response(status_code=404)

    subs_repo.set_status(db, sub["id"], "APPROVED")
    sub = subs_repo.get(db, sub["id"])

    # Welcome message via chosen channel
    settings = request.app.state.settings
    unsub_token = sign(settings.hmac_secret, purpose="unsubscribe",
                       subscriber_id=sub["id"])
    text = (
        f"Hi {sub['display_name']} — you're confirmed. ✓\n\n"
        f"You'll get your first DC crime briefing tomorrow morning at 6am, "
        f"covering the area within {sub['radius_m']:,}m of your home.\n\n"
        f"Unsubscribe anytime: {settings.base_url}/u/{sub['id']}?token={unsub_token}"
    )
    await dispatch(
        sub,
        email_notifier=request.app.state.email_notifier,
        whatsapp_notifier=request.app.state.whatsapp_notifier,
        subject=f"Welcome to wswdy, {sub['display_name']}",
        text=text, image_path=None,
    )

    return RedirectResponse(url=f"/a/{token}?done=approved",
                            status_code=303)


@router.post("/a/{token}/reject")
async def review_reject(request: Request, token: str):
    sid_or_resp = _verify_or_400(request, token)
    if isinstance(sid_or_resp, Response):
        return sid_or_resp
    db = request.app.state.db
    if not subs_repo.get(db, sid_or_resp):
        return Response(status_code=404)
    subs_repo.set_status(db, sid_or_resp, "REJECTED")
    return RedirectResponse(url=f"/a/{token}?done=rejected", status_code=303)
```

- [ ] **Step 3: Create the admin_review template**

`src/wswdy/templates/admin_review.html`:
```html
{% extends "base.html" %}
{% block title %}Review subscriber — wswdy{% endblock %}
{% block nav_meta %}admin · review{% endblock %}
{% block content %}
<main class="page fade-in" style="max-width: 540px;">
  <section class="lede">
    <div class="lede-eyebrow">Pending signup</div>
    <h1 class="display lede-title">{{ sub.display_name }} wants in.</h1>
    <p class="lede-sub">Approve to send a welcome message; reject to decline.</p>
  </section>

  <div class="card">
    <div class="card-eyebrow">request</div>
    <div class="summary">
      <div class="summary-row"><span class="k">name</span><span class="v">{{ sub.display_name }}</span></div>
      <div class="summary-row"><span class="k">channel</span><span class="v">{{ sub.preferred_channel }} <span class="mono">{{ sub.email or sub.phone }}</span></span></div>
      <div class="summary-row"><span class="k">address</span><span class="v">{{ sub.address_text }}</span></div>
      <div class="summary-row"><span class="k">coords</span><span class="v mono">{{ "%.4f"|format(sub.lat) }}, {{ "%.4f"|format(sub.lon) }}</span></div>
      <div class="summary-row"><span class="k">radius</span><span class="v tnum">{{ "{:,}".format(sub.radius_m) }}m</span></div>
      <div class="summary-row"><span class="k">submitted</span><span class="v tnum">{{ sub.created_at }}</span></div>
    </div>

    {% if sub.status == "PENDING" %}
    <div class="actions">
      <form method="post" action="/a/{{ token }}/approve" style="display:inline">
        <button type="submit" class="btn btn-primary">Approve & send welcome</button>
      </form>
      <form method="post" action="/a/{{ token }}/reject" style="display:inline">
        <button type="submit" class="btn btn-secondary">Reject</button>
      </form>
    </div>
    {% else %}
    <div class="card-eyebrow" style="margin-top:1rem;">already {{ sub.status.lower() }}</div>
    {% endif %}
  </div>
</main>
{% endblock %}
```

- [ ] **Step 4: Mount the router**

In `src/wswdy/main.py`, add `from wswdy.routes import admin_review` and `app.include_router(admin_review.router)`.

- [ ] **Step 5: Run; commit**

```bash
pytest tests/test_routes_admin_review.py -v
git add src/wswdy/routes/admin_review.py src/wswdy/templates/admin_review.html \
        src/wswdy/main.py tests/test_routes_admin_review.py
git commit -m "feat(routes): admin approve/reject + welcome message"
```

---

### Task 28: Unsubscribe routes (`src/wswdy/routes/unsubscribe.py`)

**Files:**
- Create: `src/wswdy/routes/unsubscribe.py`, `src/wswdy/templates/unsubscribe.html` (port from mockup), `tests/test_routes_unsubscribe.py`
- Modify: `src/wswdy/main.py`

- [ ] **Step 1: Write the failing test**

`tests/test_routes_unsubscribe.py`:
```python
import pytest
from fastapi.testclient import TestClient
from wswdy.main import create_app
from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import sign


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def _seed(app, sid="abc"):
    subs_repo.insert_pending(app.state.db, sid=sid, display_name="Jane",
                              email="j@x", phone=None, preferred_channel="email",
                              address_text="x", lat=38.9, lon=-77.0, radius_m=1000)
    subs_repo.set_status(app.state.db, sid, "APPROVED")
    return sign("secret", purpose="unsubscribe", subscriber_id=sid)


def test_get_unsubscribe_renders_confirmation(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/u/abc?token={token}")
    assert r.status_code == 200
    assert b"Jane" in r.content
    assert b"unsubscribe" in r.content.lower()


def test_post_unsubscribe_marks_unsubscribed(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.post(f"/u/abc?token={token}")
    assert r.status_code == 200
    s = subs_repo.get(app.state.db, "abc")
    assert s["status"] == "UNSUBSCRIBED"
    assert b"out" in r.content.lower() or b"unsubscribed" in r.content.lower()


def test_unsubscribe_token_for_other_subscriber_rejected(app):
    _seed(app, sid="abc")
    _seed(app, sid="other")
    bad_token = sign("secret", purpose="unsubscribe", subscriber_id="other")
    client = TestClient(app)
    r = client.get(f"/u/abc?token={bad_token}")
    assert r.status_code == 400


def test_unsubscribe_no_expiry(app):
    """Unsubscribe links must work indefinitely — never expire."""
    sid = "abc"
    subs_repo.insert_pending(app.state.db, sid=sid, display_name="J",
                              email="j@x", phone=None, preferred_channel="email",
                              address_text="x", lat=38.9, lon=-77.0, radius_m=1000)
    subs_repo.set_status(app.state.db, sid, "APPROVED")
    # Token signed with no TTL
    token = sign("secret", purpose="unsubscribe", subscriber_id=sid)
    client = TestClient(app)
    r = client.get(f"/u/{sid}?token={token}")
    assert r.status_code == 200
```

- [ ] **Step 2: Implement**

`src/wswdy/routes/unsubscribe.py`:
```python
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import verify, TokenError

router = APIRouter()


def _verify(request: Request, sid: str, token: str) -> bool:
    try:
        payload = verify(request.app.state.settings.hmac_secret, token,
                         purpose="unsubscribe")
        return payload["subscriber_id"] == sid
    except TokenError:
        return False


@router.get("/u/{sid}", response_class=HTMLResponse)
async def unsubscribe_get(request: Request, sid: str, token: str):
    if not _verify(request, sid, token):
        return Response(status_code=400, content="invalid token")
    sub = subs_repo.get(request.app.state.db, sid)
    if not sub:
        return Response(status_code=404)
    from wswdy.main import templates
    return templates.TemplateResponse(
        request, "unsubscribe.html",
        {"sub": sub, "token": token, "done": sub["status"] == "UNSUBSCRIBED"},
    )


@router.post("/u/{sid}", response_class=HTMLResponse)
async def unsubscribe_post(request: Request, sid: str, token: str):
    if not _verify(request, sid, token):
        return Response(status_code=400, content="invalid token")
    db = request.app.state.db
    if not subs_repo.get(db, sid):
        return Response(status_code=404)
    subs_repo.set_status(db, sid, "UNSUBSCRIBED")
    sub = subs_repo.get(db, sid)
    from wswdy.main import templates
    return templates.TemplateResponse(
        request, "unsubscribe.html", {"sub": sub, "token": token, "done": True},
    )
```

- [ ] **Step 3: Port the unsubscribe template**

`src/wswdy/templates/unsubscribe.html`:
```html
{% extends "base.html" %}
{% block title %}Unsubscribe — wswdy{% endblock %}
{% block nav_meta %}unsubscribe{% endblock %}
{% block content %}
<main class="page fade-in" style="max-width: 540px;">
  {% if not done %}
  <section class="lede">
    <div class="lede-eyebrow">Cancel briefing</div>
    <h1 class="display lede-title">
      Hey <span class="name">{{ sub.display_name }}</span> — leaving us?
    </h1>
    <p class="lede-sub">We won't try to talk you out of it. One click and you're done.</p>
  </section>
  <div class="card">
    <div class="card-eyebrow">your subscription</div>
    <div class="summary">
      <div class="summary-row"><span class="k">channel</span><span class="v">{{ sub.preferred_channel }} <span class="mono">{{ sub.email or sub.phone }}</span></span></div>
      <div class="summary-row"><span class="k">coverage</span><span class="v"><span class="tnum">{{ "{:,}".format(sub.radius_m) }}m</span> around {{ sub.address_text }}</span></div>
      <div class="summary-row"><span class="k">joined</span><span class="v tnum">{{ sub.created_at }}</span></div>
    </div>
    <div class="actions">
      <form method="post" action="/u/{{ sub.id }}?token={{ token }}" style="display:inline">
        <button type="submit" class="btn btn-danger">Yes, unsubscribe me</button>
      </form>
      <a href="/" class="keep tlink">Nevermind, keep me on</a>
    </div>
  </div>
  {% else %}
  <div class="card">
    <div class="done-mark"><svg viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg></div>
    <div class="card-eyebrow">confirmed</div>
    <h2 class="done-title">You're out. Take care, {{ sub.display_name }}.</h2>
    <p class="done-sub">No more morning briefings.</p>
    <div class="actions">
      <a href="/" class="btn btn-secondary">Re-subscribe</a>
    </div>
  </div>
  {% endif %}
</main>
{% endblock %}
```

- [ ] **Step 4: Mount router; run; commit**

In `main.py`, add `from wswdy.routes import unsubscribe` and `app.include_router(unsubscribe.router)`.

```bash
pytest tests/test_routes_unsubscribe.py -v
git add src/wswdy/routes/unsubscribe.py src/wswdy/templates/unsubscribe.html \
        src/wswdy/main.py tests/test_routes_unsubscribe.py
git commit -m "feat(routes): unsubscribe with no-expiry tokens"
```

---

### Task 29: Map view + /api/crimes (`src/wswdy/routes/map_view.py`, `api_crimes.py`)

**Files:**
- Create: `src/wswdy/routes/map_view.py`, `src/wswdy/routes/api_crimes.py`, `src/wswdy/templates/map.html` (port from mockup), `tests/test_routes_map.py`, `tests/test_routes_api_crimes.py`
- Modify: `src/wswdy/main.py`

- [ ] **Step 1: Write the failing tests**

`tests/test_routes_map.py`:
```python
import pytest
from fastapi.testclient import TestClient
from wswdy.main import create_app
from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import sign


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def _seed(app):
    subs_repo.insert_pending(app.state.db, sid="abc", display_name="Jane",
                              email="j@x", phone=None, preferred_channel="email",
                              address_text="1500 14th St NW", lat=38.9097, lon=-77.0319,
                              radius_m=1000)
    subs_repo.set_status(app.state.db, "abc", "APPROVED")
    return sign("secret", purpose="map", subscriber_id="abc")


def test_map_renders_with_valid_token(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/map/abc?token={token}")
    assert r.status_code == 200
    assert b"Jane" in r.content
    assert b"leaflet" in r.content.lower()
    assert b"MAPTILER_API_KEY" not in r.content  # injected as JS, not as literal name
    assert b"abc" in r.content


def test_map_invalid_token_400(app):
    _seed(app)
    client = TestClient(app)
    r = client.get("/map/abc?token=bad.token")
    assert r.status_code == 400
```

`tests/test_routes_api_crimes.py`:
```python
from datetime import datetime, timedelta, timezone
import pytest
from fastapi.testclient import TestClient
from wswdy.main import create_app
from wswdy.repos import subscribers as subs_repo
from wswdy.repos.crimes import upsert_many
from wswdy.tokens import sign


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def _seed(app):
    subs_repo.insert_pending(app.state.db, sid="abc", display_name="Jane",
                              email="j@x", phone=None, preferred_channel="email",
                              address_text="x", lat=38.9097, lon=-77.0319, radius_m=1000)
    subs_repo.set_status(app.state.db, "abc", "APPROVED")
    now = datetime.now(timezone.utc)
    upsert_many(app.state.db, [
        {"ccn": "near-recent", "offense": "ROBBERY", "method": "GUN", "shift": "DAY",
         "block_address": "1400 P", "lat": 38.9100, "lon": -77.0319,
         "report_dt": (now - timedelta(hours=2)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None,
         "raw_json": "{}"},
        {"ccn": "near-old", "offense": "THEFT/OTHER", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9100, "lon": -77.0319,
         "report_dt": (now - timedelta(days=5)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None,
         "raw_json": "{}"},
        {"ccn": "far", "offense": "THEFT/OTHER", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9500, "lon": -77.0500,
         "report_dt": (now - timedelta(hours=2)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None,
         "raw_json": "{}"},
    ])
    return sign("secret", purpose="map", subscriber_id="abc")


def test_api_crimes_24h_returns_recent_only(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crimes?subscriber=abc&token={token}&window=24h")
    assert r.status_code == 200
    fc = r.json()
    assert fc["type"] == "FeatureCollection"
    ccns = {f["properties"]["ccn"] for f in fc["features"]}
    assert ccns == {"near-recent"}


def test_api_crimes_7d_includes_older(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crimes?subscriber=abc&token={token}&window=7d")
    fc = r.json()
    ccns = {f["properties"]["ccn"] for f in fc["features"]}
    assert ccns == {"near-recent", "near-old"}


def test_api_crimes_features_include_tier(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crimes?subscriber=abc&token={token}&window=24h")
    f = r.json()["features"][0]
    assert "tier" in f["properties"]
    assert f["properties"]["tier"] == 1  # ROBBERY + GUN


def test_api_crimes_invalid_token_401(app):
    _seed(app)
    client = TestClient(app)
    r = client.get("/api/crimes?subscriber=abc&token=bad&window=24h")
    assert r.status_code == 401


def test_api_crimes_invalid_window_400(app):
    token = _seed(app)
    client = TestClient(app)
    r = client.get(f"/api/crimes?subscriber=abc&token={token}&window=year")
    assert r.status_code == 400
```

- [ ] **Step 2: Implement /api/crimes**

`src/wswdy/routes/api_crimes.py`:
```python
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from wswdy.repos import subscribers as subs_repo
from wswdy.repos.crimes import list_in_radius_window
from wswdy.tiers import classify
from wswdy.tokens import verify, TokenError

router = APIRouter()

_WINDOWS = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}


@router.get("/api/crimes")
async def api_crimes(request: Request, subscriber: str, token: str, window: str = "24h"):
    secret = request.app.state.settings.hmac_secret
    try:
        payload = verify(secret, token, purpose="map")
    except TokenError as e:
        return Response(status_code=401, content=f"invalid token: {e}")
    if payload["subscriber_id"] != subscriber:
        return Response(status_code=401, content="token mismatch")
    if window not in _WINDOWS:
        return Response(status_code=400, content="unknown window")

    sub = subs_repo.get(request.app.state.db, subscriber)
    if not sub:
        return Response(status_code=404)

    now = datetime.now(timezone.utc)
    start = (now - _WINDOWS[window]).isoformat(timespec="seconds")
    end = now.isoformat(timespec="seconds")

    rows = list_in_radius_window(
        request.app.state.db, sub["lat"], sub["lon"], sub["radius_m"],
        start=start, end=end,
    )
    features = [{
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
        "properties": {
            "ccn": r["ccn"], "offense": r["offense"], "method": r["method"],
            "block": r["block_address"], "report_dt": r["report_dt"],
            "tier": classify(r["offense"], r["method"]),
        },
    } for r in rows]
    return JSONResponse({"type": "FeatureCollection", "features": features})
```

- [ ] **Step 3: Implement /map/{id}**

`src/wswdy/routes/map_view.py`:
```python
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from wswdy.repos import subscribers as subs_repo
from wswdy.tokens import verify, TokenError

router = APIRouter()


@router.get("/map/{sid}", response_class=HTMLResponse)
async def map_view(request: Request, sid: str, token: str):
    settings = request.app.state.settings
    try:
        payload = verify(settings.hmac_secret, token, purpose="map")
    except TokenError as e:
        return Response(status_code=400, content=f"invalid token: {e}")
    if payload["subscriber_id"] != sid:
        return Response(status_code=400, content="token mismatch")

    sub = subs_repo.get(request.app.state.db, sid)
    if not sub:
        return Response(status_code=404)

    from wswdy.main import templates
    return templates.TemplateResponse(
        request, "map.html",
        {"sub": sub, "token": token, "maptiler_key": settings.maptiler_api_key},
    )
```

- [ ] **Step 4: Port the map template**

`src/wswdy/templates/map.html`:
```html
{% extends "base.html" %}
{% block title %}Map — wswdy{% endblock %}
{% block head_extra %}
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<style>html, body { height: 100%; overflow: hidden; }</style>
{% endblock %}
{% block content %}
<aside class="panel">
  <div class="panel-head">
    <div class="panel-eyebrow">briefing for</div>
    <h1 class="panel-title">{{ sub.display_name }} <span class="accent">·</span> {{ sub.address_text }}</h1>
    <div class="panel-meta"><strong class="tnum">{{ "{:,}".format(sub.radius_m) }}m</strong> around your home</div>
  </div>
  <div class="window">
    <div class="window-label">window</div>
    <div class="toggle" id="toggle">
      <button class="active" data-window="24h"><span class="num" id="n-24h">—</span><span class="lbl">24h</span></button>
      <button data-window="7d"><span class="num" id="n-7d">—</span><span class="lbl">7d</span></button>
      <button data-window="30d"><span class="num" id="n-30d">—</span><span class="lbl">30d</span></button>
    </div>
  </div>
  <div class="legend">
    <div class="legend-head">
      <span class="legend-eyebrow">reports shown</span>
      <span class="legend-total"><span id="legend-total">—</span><span class="unit">total</span></span>
    </div>
    <div class="legend-rows">
      <div class="legend-row"><span class="swatch" style="background: var(--t1);"></span><span class="lbl">Violent</span><span class="num" id="leg-1">0</span></div>
      <div class="legend-row"><span class="swatch" style="background: var(--t2);"></span><span class="lbl">Property</span><span class="num" id="leg-2">0</span></div>
      <div class="legend-row"><span class="swatch" style="background: var(--t3);"></span><span class="lbl">Vehicle</span><span class="num" id="leg-3">0</span></div>
      <div class="legend-row"><span class="swatch" style="background: var(--t4);"></span><span class="lbl">Petty</span><span class="num" id="leg-4">0</span></div>
    </div>
  </div>
</aside>
<div id="map"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const HOME = [{{ sub.lat }}, {{ sub.lon }}];
const RADIUS_M = {{ sub.radius_m }};
const SID = "{{ sub.id }}";
const TOKEN = "{{ token }}";
const KEY = "{{ maptiler_key }}";

const map = L.map('map', { center: HOME, zoom: 15 });
L.tileLayer(`https://api.maptiler.com/maps/streets-v2/{z}/{x}/{y}.png?key=${KEY}`, {
  attribution: '© MapTiler © OpenStreetMap · MPD DC', maxZoom: 19,
}).addTo(map);

const homeIcon = L.divIcon({ className: '', html: '<div class="home-marker"></div>',
  iconSize: [26, 26], iconAnchor: [13, 13] });
L.marker(HOME, { icon: homeIcon }).addTo(map);
L.circle(HOME, { radius: RADIUS_M, className: 'radius-circle' }).addTo(map);

let layer = L.layerGroup().addTo(map);

async function load(window) {
  const r = await fetch(`/api/crimes?subscriber=${SID}&token=${encodeURIComponent(TOKEN)}&window=${window}`);
  if (!r.ok) return;
  const fc = await r.json();
  layer.clearLayers();
  const counts = [0,0,0,0];
  fc.features.forEach(f => {
    const t = f.properties.tier;
    counts[t-1]++;
    const icon = L.divIcon({ className: '',
      html: `<div class="marker t${t}"></div>`,
      iconSize: [18,18], iconAnchor: [9,9] });
    L.marker([f.geometry.coordinates[1], f.geometry.coordinates[0]], { icon })
      .addTo(layer)
      .bindPopup(`<div class="pop-title">${f.properties.offense}</div>
                  <div class="pop-row"><span class="k">where</span><span class="v">${f.properties.block || ''}</span></div>
                  <div class="pop-row"><span class="k">when</span><span class="v">${f.properties.report_dt}</span></div>`);
  });
  document.getElementById('legend-total').textContent = fc.features.length;
  for (let i=1; i<=4; i++) document.getElementById('leg-'+i).textContent = counts[i-1];
  document.getElementById('n-' + window).textContent = fc.features.length;
}

document.querySelectorAll('#toggle button').forEach(b => {
  b.addEventListener('click', () => {
    document.querySelectorAll('#toggle button').forEach(x => x.classList.remove('active'));
    b.classList.add('active');
    load(b.dataset.window);
  });
});

// Pre-fetch all three counts on load
['24h', '7d', '30d'].forEach(load);
</script>
{% endblock %}
```

- [ ] **Step 5: Mount routers; run; commit**

In `main.py`:
```python
from wswdy.routes import map_view, api_crimes
app.include_router(map_view.router)
app.include_router(api_crimes.router)
```

```bash
pytest tests/test_routes_map.py tests/test_routes_api_crimes.py -v
git add src/wswdy/routes/map_view.py src/wswdy/routes/api_crimes.py \
        src/wswdy/templates/map.html src/wswdy/main.py \
        tests/test_routes_map.py tests/test_routes_api_crimes.py
git commit -m "feat(routes): map view + /api/crimes GeoJSON endpoint"
```

---

### Task 30: /api/preview (`src/wswdy/routes/api_preview.py`)

**Files:**
- Create: `src/wswdy/routes/api_preview.py`, `tests/test_routes_api_preview.py`
- Modify: `src/wswdy/main.py`

- [ ] **Step 1: Write the failing test**

`tests/test_routes_api_preview.py`:
```python
from datetime import datetime, timedelta, timezone
import pytest
from fastapi.testclient import TestClient
from wswdy.main import create_app
from wswdy.repos.crimes import upsert_many


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "admin")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def _seed(app):
    now = datetime.now(timezone.utc)
    upsert_many(app.state.db, [
        {"ccn": "1", "offense": "THEFT/OTHER", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9100, "lon": -77.0319,
         "report_dt": (now - timedelta(days=2)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None, "raw_json": "{}"},
        {"ccn": "2", "offense": "ROBBERY", "method": "GUN", "shift": "DAY",
         "block_address": "x", "lat": 38.9100, "lon": -77.0319,
         "report_dt": (now - timedelta(days=4)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None, "raw_json": "{}"},
        {"ccn": "3", "offense": "BURGLARY", "method": None, "shift": "DAY",
         "block_address": "x", "lat": 38.9500, "lon": -77.0500,  # far
         "report_dt": (now - timedelta(days=1)).isoformat(),
         "start_dt": None, "end_dt": None, "ward": None, "district": None, "raw_json": "{}"},
    ])


def test_preview_returns_aggregate_counts(app):
    _seed(app)
    client = TestClient(app)
    r = client.post("/api/preview",
                    json={"lat": 38.9097, "lon": -77.0319, "radius_m": 500})
    assert r.status_code == 200
    d = r.json()
    assert d["window_days"] == 7
    assert d["total"] == 2
    assert d["by_tier"]["1"] == 1
    assert d["by_tier"]["4"] == 1
    assert d["avg_per_day"] == pytest.approx(2/7, abs=0.01)


def test_preview_validates_radius(app):
    client = TestClient(app)
    r = client.post("/api/preview", json={"lat": 38.9, "lon": -77.0, "radius_m": 50})
    assert r.status_code == 400


def test_preview_outside_dc_rejected(app):
    client = TestClient(app)
    r = client.post("/api/preview", json={"lat": 39.29, "lon": -76.62, "radius_m": 500})
    assert r.status_code == 400


def test_preview_rate_limited(app):
    client = TestClient(app)
    body = {"lat": 38.9097, "lon": -77.0319, "radius_m": 500}
    for _ in range(30):
        client.post("/api/preview", json=body)
    r = client.post("/api/preview", json=body)
    assert r.status_code == 429
```

- [ ] **Step 2: Implement**

`src/wswdy/routes/api_preview.py`:
```python
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from wswdy.geo import in_dc_bbox
from wswdy.ratelimit import RateLimiter
from wswdy.repos.crimes import list_in_radius_window
from wswdy.tiers import classify

router = APIRouter()
_rl = RateLimiter(max_requests=30, window_s=60)


class PreviewBody(BaseModel):
    lat: float
    lon: float
    radius_m: int = Field(ge=200, le=2000)


@router.post("/api/preview")
async def api_preview(request: Request, body: PreviewBody):
    ip = request.client.host if request.client else "unknown"
    if not _rl.check(ip):
        return Response(status_code=429, content="rate limited")
    if not in_dc_bbox(body.lat, body.lon):
        return Response(status_code=400, content="coordinates outside DC")

    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=7)).isoformat(timespec="seconds")
    end = now.isoformat(timespec="seconds")
    rows = list_in_radius_window(
        request.app.state.db, body.lat, body.lon, body.radius_m,
        start=start, end=end,
    )
    counts = {1: 0, 2: 0, 3: 0, 4: 0}
    for r in rows:
        counts[classify(r["offense"], r["method"])] += 1
    total = len(rows)
    return JSONResponse({
        "window_days": 7,
        "total": total,
        "avg_per_day": total / 7,
        "by_tier": {str(k): v for k, v in counts.items()},
    })
```

- [ ] **Step 3: Mount router; run; commit**

In `main.py`:
```python
from wswdy.routes import api_preview
app.include_router(api_preview.router)
```

```bash
pytest tests/test_routes_api_preview.py -v
git add src/wswdy/routes/api_preview.py src/wswdy/main.py tests/test_routes_api_preview.py
git commit -m "feat(routes): /api/preview aggregate counts with rate limit"
```

---

### Task 31: /admin dashboard (`src/wswdy/routes/admin.py`)

**Files:**
- Create: `src/wswdy/routes/admin.py`, `src/wswdy/templates/admin.html`, `tests/test_routes_admin.py`
- Modify: `src/wswdy/main.py`

- [ ] **Step 1: Write the failing test**

`tests/test_routes_admin.py`:
```python
import pytest
from fastapi.testclient import TestClient
from wswdy.main import create_app
from wswdy.repos.subscribers import insert_pending, set_status
from wswdy.repos.fetch_log import record_success
from wswdy.repos.send_log import record


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "ADMINTOKEN123")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "t.db"))
    return create_app()


def test_admin_no_token_rejected(app):
    client = TestClient(app)
    r = client.get("/admin")
    assert r.status_code in (401, 403)


def test_admin_wrong_token_rejected(app):
    client = TestClient(app)
    r = client.get("/admin?token=wrong")
    assert r.status_code in (401, 403)


def test_admin_valid_token_renders(app):
    insert_pending(app.state.db, sid="a", display_name="A", email="a@x", phone=None,
                   preferred_channel="email", address_text="x",
                   lat=38.9, lon=-77.0, radius_m=1000)
    set_status(app.state.db, "a", "APPROVED")
    record_success(app.state.db, added=42, updated=3)
    record(app.state.db, "a", "2026-04-28", "email", "sent")

    client = TestClient(app)
    r = client.get("/admin?token=ADMINTOKEN123")
    assert r.status_code == 200
    assert b"42" in r.content
    assert b"approved" in r.content.lower() or b"APPROVED" in r.content
```

- [ ] **Step 2: Implement**

`src/wswdy/routes/admin.py`:
```python
from datetime import date
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, Response

from wswdy.repos.fetch_log import last_attempt
from wswdy.repos.send_log import recent_failures, send_volume_last_n_days
from wswdy.repos.subscribers import list_by_status

router = APIRouter()


@router.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request, token: str = ""):
    expected = request.app.state.settings.admin_token
    if not token or token != expected:
        return Response(status_code=401, content="unauthorized")

    db = request.app.state.db
    from wswdy.main import templates
    return templates.TemplateResponse(request, "admin.html", {
        "pending": list_by_status(db, "PENDING"),
        "approved": list_by_status(db, "APPROVED"),
        "rejected": list_by_status(db, "REJECTED"),
        "unsubscribed": list_by_status(db, "UNSUBSCRIBED"),
        "last_fetch": last_attempt(db),
        "send_volume": send_volume_last_n_days(db, n=7, today=str(date.today())),
        "failures": recent_failures(db, limit=20),
        "token": token,
    })
```

- [ ] **Step 3: Create the admin template**

`src/wswdy/templates/admin.html`:
```html
{% extends "base.html" %}
{% block title %}admin · wswdy{% endblock %}
{% block nav_meta %}admin{% endblock %}
{% block content %}
<main class="page fade-in" style="max-width: 980px;">
  <h1 class="display" style="font-size: 2rem; margin: 0 0 1.5rem;">Admin</h1>

  <div class="card" style="margin-bottom:1.25rem;">
    <h2 class="card-title">Subscribers</h2>
    <div class="summary">
      <div class="summary-row"><span class="k">approved</span><span class="v tnum">{{ approved|length }}</span></div>
      <div class="summary-row"><span class="k">pending</span><span class="v tnum">{{ pending|length }}</span></div>
      <div class="summary-row"><span class="k">rejected</span><span class="v tnum">{{ rejected|length }}</span></div>
      <div class="summary-row"><span class="k">unsubscribed</span><span class="v tnum">{{ unsubscribed|length }}</span></div>
    </div>
    {% if pending %}
    <h3 class="card-eyebrow" style="margin-top:1.25rem;">pending</h3>
    <ul style="list-style:none; padding:0; margin: 0.5rem 0 0;">
      {% for s in pending %}
      <li style="padding:0.4rem 0; border-bottom: 1px solid var(--border);">
        <strong>{{ s.display_name }}</strong> · {{ s.preferred_channel }} · {{ s.address_text }}
        ({{ s.radius_m }}m) · <span class="mono" style="font-size:.78rem;">{{ s.created_at }}</span>
      </li>
      {% endfor %}
    </ul>
    {% endif %}
  </div>

  <div class="card" style="margin-bottom:1.25rem;">
    <h2 class="card-title">MPD fetch</h2>
    {% if last_fetch %}
      <div class="summary-row"><span class="k">status</span><span class="v">{{ last_fetch.status }}</span></div>
      <div class="summary-row"><span class="k">when</span><span class="v tnum">{{ last_fetch.fetched_at }}</span></div>
      <div class="summary-row"><span class="k">added / updated</span><span class="v tnum">+{{ last_fetch.crimes_added or 0 }} / ~{{ last_fetch.crimes_updated or 0 }}</span></div>
      {% if last_fetch.error %}<div class="summary-row"><span class="k">error</span><span class="v">{{ last_fetch.error }}</span></div>{% endif %}
    {% else %}<p>No fetches recorded.</p>{% endif %}
  </div>

  <div class="card" style="margin-bottom:1.25rem;">
    <h2 class="card-title">Send volume (last 7 days)</h2>
    <table style="width:100%; border-collapse:collapse;">
      <thead><tr><th style="text-align:left;">Date</th><th>Sent</th><th>Failed</th><th>Skipped</th></tr></thead>
      <tbody>
      {% for r in send_volume %}
      <tr><td class="tnum">{{ r.send_date }}</td><td class="tnum">{{ r.sent }}</td><td class="tnum">{{ r.failed }}</td><td class="tnum">{{ r.skipped }}</td></tr>
      {% endfor %}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2 class="card-title">Recent failures</h2>
    {% if failures %}
    <ul style="list-style:none; padding:0;">
      {% for f in failures %}
      <li style="padding:0.4rem 0; border-bottom: 1px solid var(--border);">
        <span class="mono" style="font-size:.78rem;">{{ f.sent_at }}</span> ·
        {{ f.subscriber_id }} ({{ f.channel }}): {{ f.error }}
      </li>
      {% endfor %}
    </ul>
    {% else %}<p style="color: var(--ink-3);">No failures.</p>{% endif %}
  </div>
</main>
{% endblock %}
```

- [ ] **Step 4: Mount; run; commit**

In `main.py`:
```python
from wswdy.routes import admin
app.include_router(admin.router)
```

```bash
pytest tests/test_routes_admin.py -v
git add src/wswdy/routes/admin.py src/wswdy/templates/admin.html src/wswdy/main.py \
        tests/test_routes_admin.py
git commit -m "feat(routes): /admin read-only dashboard with static-token auth"
```

**End of Phase 6.** All HTTP routes are implemented and tested.

---

## Phase 7 — Scheduler integration (Task 32)

### Task 32: Wire APScheduler into the FastAPI lifespan (`src/wswdy/scheduler.py`)

**Files:**
- Create: `src/wswdy/scheduler.py`, `tests/test_scheduler.py`
- Modify: `src/wswdy/main.py`

- [ ] **Step 1: Write the failing test**

`tests/test_scheduler.py`:
```python
from datetime import datetime
from unittest.mock import AsyncMock
import pytest
from apscheduler.triggers.cron import CronTrigger
from wswdy.scheduler import build_scheduler, JOB_IDS


def test_build_scheduler_registers_all_jobs():
    fetch = AsyncMock()
    send = AsyncMock()
    prune = AsyncMock()
    health = AsyncMock()
    s = build_scheduler(fetch_fn=fetch, send_fn=send, prune_fn=prune, health_fn=health)
    job_ids = {j.id for j in s.get_jobs()}
    assert job_ids == set(JOB_IDS)


def test_jobs_use_eastern_time():
    fetch = AsyncMock(); send = AsyncMock(); prune = AsyncMock(); health = AsyncMock()
    s = build_scheduler(fetch_fn=fetch, send_fn=send, prune_fn=prune, health_fn=health)
    for j in s.get_jobs():
        assert isinstance(j.trigger, CronTrigger)
        # zoneinfo timezone string
        assert "New_York" in str(j.trigger.timezone)


def test_jobs_have_expected_times():
    fetch = AsyncMock(); send = AsyncMock(); prune = AsyncMock(); health = AsyncMock()
    s = build_scheduler(fetch_fn=fetch, send_fn=send, prune_fn=prune, health_fn=health)
    times = {j.id: str(j.trigger) for j in s.get_jobs()}
    assert "hour='3'" in times["prune"]
    assert "hour='5'" in times["fetch"] and "minute='30'" in times["fetch"]
    assert "hour='6'" in times["send"]
    assert "hour='23'" in times["health"]
```

- [ ] **Step 2: Implement the scheduler**

`src/wswdy/scheduler.py`:
```python
"""APScheduler setup — registers the four daily jobs in ET."""
from typing import Awaitable, Callable
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
JOB_IDS = ("prune", "fetch", "send", "health")


def build_scheduler(
    *, fetch_fn: Callable[[], Awaitable[None]],
    send_fn: Callable[[], Awaitable[None]],
    prune_fn: Callable[[], Awaitable[None]],
    health_fn: Callable[[], Awaitable[None]],
) -> AsyncIOScheduler:
    s = AsyncIOScheduler(timezone=ET)
    s.add_job(prune_fn, CronTrigger(hour=3, minute=0, timezone=ET), id="prune")
    s.add_job(fetch_fn, CronTrigger(hour=5, minute=30, timezone=ET), id="fetch")
    s.add_job(send_fn,  CronTrigger(hour=6, minute=0, timezone=ET), id="send")
    s.add_job(health_fn, CronTrigger(hour=23, minute=0, timezone=ET), id="health")
    return s
```

- [ ] **Step 3: Wire scheduler into main.py via lifespan**

Update `src/wswdy/main.py`:
```python
"""FastAPI application factory with scheduler lifespan."""
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from wswdy.alerts import AdminAlerter
from wswdy.config import get_settings
from wswdy.db import connect, init_schema
from wswdy.jobs.fetch import run_fetch
from wswdy.jobs.send import run_daily_sends
from wswdy.jobs.prune import run_prune
from wswdy.jobs.health import run_health_snapshot
from wswdy.notifiers.email import EmailNotifier
from wswdy.notifiers.whatsapp import WhatsAppMcpNotifier
from wswdy.routes import (
    health, public, admin_review, unsubscribe, map_view, api_crimes, api_preview, admin,
)
from wswdy.scheduler import build_scheduler

PKG_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PKG_DIR / "templates"
STATIC_DIR = PKG_DIR / "static"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = app.state.settings

    async def fetch_job():
        await run_fetch(
            db=app.state.db, feed_url=str(settings.mpd_feed_url),
            alerter=app.state.alerter, fixture_path=settings.fixture_mpd_path,
        )

    async def send_job():
        from wswdy.clients.maptiler import render_static_map

        async def render(*, center_lat, center_lon, radius_m, markers, out_path):
            return await render_static_map(
                api_key=settings.maptiler_api_key,
                center_lat=center_lat, center_lon=center_lon,
                radius_m=radius_m, markers=markers, out_path=out_path,
            )

        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        send_date = str(date.today())
        await run_daily_sends(
            db=app.state.db,
            email=app.state.email_notifier,
            whatsapp=app.state.whatsapp_notifier,
            alerter=app.state.alerter,
            base_url=settings.base_url,
            hmac_secret=settings.hmac_secret,
            send_date=send_date, now_iso=now_iso,
            stagger=(settings.env != "dev"),
            render_static_map=render,
            static_map_dir=Path(settings.log_dir) / "static_maps",
        )

    async def prune_job():
        run_prune(app.state.db,
                  today_iso=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                  days=90)

    async def health_job():
        await run_health_snapshot(
            db=app.state.db, email=app.state.email_notifier,
            admin_email=settings.admin_email, today=str(date.today()),
        )

    scheduler = build_scheduler(
        fetch_fn=fetch_job, send_fn=send_job,
        prune_fn=prune_job, health_fn=health_job,
    )
    scheduler.start()
    app.state.scheduler = scheduler
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        app.state.db.close()


def create_app() -> FastAPI:
    settings = get_settings()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    app = FastAPI(title="wswdy", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.db = connect(settings.db_path)
    init_schema(app.state.db)
    app.state.email_notifier = EmailNotifier(
        host=settings.smtp_host, port=settings.smtp_port,
        user=settings.smtp_user, password=settings.smtp_pass,
        sender=settings.smtp_from,
    )
    app.state.whatsapp_notifier = WhatsAppMcpNotifier(
        base_url=settings.whatsapp_mcp_url, token=settings.whatsapp_mcp_token,
    )
    app.state.alerter = AdminAlerter(
        db=app.state.db, email=app.state.email_notifier,
        admin_email=settings.admin_email, ha_webhook_url=settings.ha_webhook_url,
    )

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(health.router)
    app.include_router(public.router)
    app.include_router(admin_review.router)
    app.include_router(unsubscribe.router)
    app.include_router(map_view.router)
    app.include_router(api_crimes.router)
    app.include_router(api_preview.router)
    app.include_router(admin.router)
    return app


app = create_app()
```

- [ ] **Step 4: Run all tests; commit**

```bash
pytest -v
git add src/wswdy/scheduler.py src/wswdy/main.py tests/test_scheduler.py
git commit -m "feat(scheduler): APScheduler with four daily jobs in ET via lifespan"
```

**End of Phase 7.** App is feature-complete; scheduler runs the daily jobs.

---

## Phase 8 — Deploy & ops (Tasks 33–35)

### Task 33: systemd unit + Cloudflare Tunnel config + deploy notes

**Files:**
- Create: `deploy/dccrime.service`, `deploy/cloudflared-config.yml.example`, `deploy/logrotate.conf`, `docs/deploy.md`

- [ ] **Step 1: Write systemd unit**

`deploy/dccrime.service`:
```ini
[Unit]
Description=wswdy — DC Crime Alerts
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=wswdy
Group=wswdy
WorkingDirectory=/opt/wswdy
EnvironmentFile=/opt/wswdy/.env
ExecStart=/opt/wswdy/.venv/bin/uvicorn wswdy.main:app --host 127.0.0.1 --port 8000 --workers 1
Restart=always
RestartSec=5
StandardOutput=append:/var/log/dccrime/app.log
StandardError=append:/var/log/dccrime/app.log
# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/wswdy /var/log/dccrime

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Cloudflare Tunnel config example**

`deploy/cloudflared-config.yml.example`:
```yaml
tunnel: <UUID-FROM-cloudflared-tunnel-create>
credentials-file: /etc/cloudflared/<UUID>.json

ingress:
  - hostname: dccrime.iandmuir.com
    service: http://127.0.0.1:8000
  - service: http_status:404
```

- [ ] **Step 3: logrotate config**

`deploy/logrotate.conf`:
```
/var/log/dccrime/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 0640 wswdy wswdy
    sharedscripts
}
```

- [ ] **Step 4: Deploy runbook**

`docs/deploy.md`:
````markdown
# Deploying wswdy to the NUC

## Prerequisites
- Proxmox LXC: Debian 12 minimal, 1 vCPU, 512 MB RAM, 4 GB disk
- Domain `iandmuir.com` on Cloudflare
- Existing WhatsApp MCP LXC reachable from this LXC

## 1. Create the LXC
```bash
# On the Proxmox host:
pct create <ID> local:vztmpl/debian-12-standard.tar.zst \
  --hostname dc-crime-app --memory 512 --cores 1 --rootfs local-lvm:4 \
  --net0 name=eth0,bridge=vmbr0,ip=dhcp \
  --features nesting=1 --unprivileged 1
pct start <ID>
pct enter <ID>
```

## 2. System setup
```bash
apt-get update && apt-get install -y python3.12 python3.12-venv git curl
adduser --system --group --home /opt/wswdy wswdy
mkdir -p /var/log/dccrime
chown wswdy:wswdy /var/log/dccrime
```

## 3. Clone + install
```bash
sudo -u wswdy bash -c "
  cd /opt/wswdy &&
  git clone https://github.com/iandmuir/dc-crime.git . &&
  python3.12 -m venv .venv &&
  .venv/bin/pip install -r requirements.txt &&
  .venv/bin/pip install -e .
"
```

## 4. Configure secrets
```bash
sudo -u wswdy cp /opt/wswdy/.env.example /opt/wswdy/.env
chmod 600 /opt/wswdy/.env
$EDITOR /opt/wswdy/.env  # fill in MAPTILER_API_KEY, SMTP, MCP, HMAC_SECRET, etc.

# Generate strong secrets:
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'  # → HMAC_SECRET
python3 -c 'import secrets; print(secrets.token_urlsafe(24))'  # → ADMIN_TOKEN
```

## 5. Install systemd unit
```bash
cp /opt/wswdy/deploy/dccrime.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now dccrime
systemctl status dccrime
curl -s http://127.0.0.1:8000/healthz   # should print {"status":"ok"}
```

## 6. Logrotate
```bash
cp /opt/wswdy/deploy/logrotate.conf /etc/logrotate.d/dccrime
logrotate -d /etc/logrotate.d/dccrime  # dry-run check
```

## 7. Cloudflare Tunnel
```bash
# Install cloudflared in this LXC
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb -o /tmp/cf.deb
dpkg -i /tmp/cf.deb

# One-time auth
cloudflared tunnel login
cloudflared tunnel create dccrime
# Note the UUID printed.

# Configure
mkdir -p /etc/cloudflared
cp /opt/wswdy/deploy/cloudflared-config.yml.example /etc/cloudflared/config.yml
$EDITOR /etc/cloudflared/config.yml   # fill in UUID
cloudflared tunnel route dns dccrime dccrime.iandmuir.com
cloudflared service install
systemctl enable --now cloudflared
```

## 8. Verify externally
```bash
curl -s https://dccrime.iandmuir.com/healthz
# (Optionally not exposed — can leave /healthz on 127.0.0.1 only by gating in cloudflared.)
```

## 9. First subscriber
- Visit `https://dccrime.iandmuir.com/`
- Submit your own signup (Ian — `iandmuir@gmail.com`)
- Approve the request via the admin email link
- Tomorrow at 06:00 ET, expect the first digest.
````

- [ ] **Step 5: Commit**

```bash
git add deploy docs/deploy.md
git commit -m "docs(deploy): systemd unit, cloudflared config, deploy runbook"
```

---

### Task 34: Local dev tooling — seed script + fixture

**Files:**
- Create: `scripts/seed.py`

- [ ] **Step 1: Implement seed script**

`scripts/seed.py`:
```python
"""Seed the local DB with synthetic subscribers + crimes for UI testing.

Usage: python scripts/seed.py
Reads WSWDY_DB_PATH from .env — make sure you're running against a dev DB.
"""
import json
import os
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
    import random
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
```

- [ ] **Step 2: Verify and commit**

```bash
WSWDY_DB_PATH=./dccrime-dev.db HMAC_SECRET=x ADMIN_TOKEN=y MAPTILER_API_KEY=z \
  python scripts/seed.py
ls -la dccrime-dev.db && rm dccrime-dev.db dccrime-dev.db-wal dccrime-dev.db-shm 2>/dev/null
git add scripts/seed.py
git commit -m "chore(scripts): seed script for local UI testing"
```

---

### Task 35: Backup script + operations runbook

**Files:**
- Create: `scripts/backup.sh`, `docs/operations.md`

- [ ] **Step 1: Backup script**

`scripts/backup.sh`:
```bash
#!/usr/bin/env bash
# Daily SQLite + .env backup. Designed to be invoked from cron at ~02:00 ET.
# Usage: backup.sh <rclone-remote-name>:<path>
# E.g.:  backup.sh gdrive:wswdy-backups
set -euo pipefail

DEST="${1:?usage: backup.sh <rclone-remote:path>}"
APP_DIR="${WSWDY_APP_DIR:-/opt/wswdy}"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cp "$APP_DIR/dccrime.db" "$WORK/dccrime.db"
sqlite3 "$WORK/dccrime.db" "PRAGMA wal_checkpoint(TRUNCATE);" >/dev/null
cp "$APP_DIR/.env" "$WORK/.env"
tar -C "$WORK" -czf "$WORK/wswdy-$TS.tar.gz" dccrime.db .env
rclone copy "$WORK/wswdy-$TS.tar.gz" "$DEST/" --quiet

# Retention: keep last 14
rclone lsf "$DEST/" --include "wswdy-*.tar.gz" | sort | head -n -14 | while read -r f; do
  rclone delete "$DEST/$f" --quiet || true
done
```

- [ ] **Step 2: Operations runbook**

`docs/operations.md`:
````markdown
# wswdy — Operations Runbook

## Daily checks (passive)
- Watch for the **23:00 ET health email**. If it doesn't arrive, log in to the LXC.
- Watch for any `[wswdy] <alert_type>` admin alert email or HA push.

## On admin alert: `mpd_down`
The MPD feed has been unreachable for >1 hour.
1. `curl -sS "https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/FeatureServer/39/query?outFields=*&where=1%3D1&resultRecordCount=1&f=geojson" | head`
2. If MPD is down externally → wait. The next morning's digests will show the "MPD data may be delayed" warning. Suppression auto-clears after 6h.
3. If MPD is back but our cron didn't catch up → SSH in and run a manual fetch:
   ```bash
   sudo -u wswdy /opt/wswdy/.venv/bin/python -c \
     "import asyncio; from wswdy.config import get_settings; \
      from wswdy.db import connect, init_schema; \
      from wswdy.alerts import AdminAlerter; \
      from wswdy.notifiers.email import EmailNotifier; \
      from wswdy.jobs.fetch import run_fetch; \
      s = get_settings(); db = connect(s.db_path); init_schema(db); \
      e = EmailNotifier(host=s.smtp_host, port=s.smtp_port, user=s.smtp_user, password=s.smtp_pass, sender=s.smtp_from); \
      a = AdminAlerter(db=db, email=e, admin_email=s.admin_email, ha_webhook_url=s.ha_webhook_url); \
      asyncio.run(run_fetch(db=db, feed_url=str(s.mpd_feed_url), alerter=a))"
   ```

## On admin alert: `whatsapp_session_expired`
The MCP needs a fresh QR scan.
1. Open the device that holds the +12024682709 WhatsApp Business session.
2. Settings → Linked Devices → Link a Device → scan the QR shown by the MCP UI in its LXC.
3. Verify with a manual welcome message via `/admin` console (or by approving a pending sub).

## On admin alert: SMTP failure
The job retried for 6h. Check the SMTP provider's status page; rotate creds if needed; restart the service:
```bash
systemctl restart dccrime
```

## Service restart
```bash
systemctl restart dccrime
journalctl -u dccrime -n 50
```

## Tunnel down
```bash
systemctl status cloudflared
journalctl -u cloudflared -n 50
# If the tunnel is down, the daily SEND still runs (no inbound HTTP needed).
# Map links and signup are unreachable until the tunnel returns.
```

## Restoring from backup
```bash
systemctl stop dccrime
cd /tmp && rclone copy gdrive:wswdy-backups/wswdy-<TS>.tar.gz .
tar -xzf wswdy-<TS>.tar.gz
sudo -u wswdy cp dccrime.db /opt/wswdy/dccrime.db
sudo -u wswdy cp .env       /opt/wswdy/.env
systemctl start dccrime
```

## Removing a subscriber manually
```bash
sqlite3 /opt/wswdy/dccrime.db \
  "UPDATE subscribers SET status='UNSUBSCRIBED', unsubscribed_at=CURRENT_TIMESTAMP WHERE id='<sid>';"
```

## Migrating to WhatsApp Cloud API later
Implement `WhatsAppCloudNotifier(Notifier)` in `src/wswdy/notifiers/whatsapp_cloud.py`,
swap the binding in `main.py:create_app()`. The Notifier protocol is the only seam.
````

- [ ] **Step 3: Commit**

```bash
git add scripts/backup.sh docs/operations.md
chmod +x scripts/backup.sh 2>/dev/null || true
git commit -m "docs(ops): backup script + operations runbook"
```

**End of Phase 8.** Project is fully deployable and operable.

---

## Phase 9 — End-to-end smoke test (Task 36)

### Task 36: E2E smoke test

**Files:**
- Create: `tests/test_e2e_smoke.py`

This test exercises the full happy path with a real in-memory app: signup → admin approve → welcome message → manual send job → digest delivered → unsubscribe.

- [ ] **Step 1: Write the test**

`tests/test_e2e_smoke.py`:
```python
"""End-to-end happy path with no real network.

Stubs MapTiler geocoding and replaces real notifiers with FakeNotifier so the
test runs offline. Exercises every public surface in sequence.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch
import pytest
from fastapi.testclient import TestClient
from wswdy.main import create_app
from wswdy.notifiers.fake import FakeNotifier
from wswdy.repos import subscribers as subs_repo
from wswdy.repos.crimes import upsert_many


@pytest.fixture
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("HMAC_SECRET", "secret")
    monkeypatch.setenv("ADMIN_TOKEN", "ADMINTOK")
    monkeypatch.setenv("MAPTILER_API_KEY", "k")
    monkeypatch.setenv("WSWDY_DB_PATH", str(tmp_path / "smoke.db"))
    monkeypatch.setenv("WSWDY_BASE_URL", "https://x.test")
    monkeypatch.setenv("ADMIN_EMAIL", "ian@test")
    return create_app()


def _seed_recent_crime(app, hours_ago=2):
    when = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat(timespec="seconds")
    upsert_many(app.state.db, [{
        "ccn": "SMOKE1", "offense": "ROBBERY", "method": "GUN", "shift": "DAY",
        "block_address": "1400 block of P St NW", "lat": 38.9100, "lon": -77.0319,
        "report_dt": when, "start_dt": None, "end_dt": None,
        "ward": "2", "district": "3", "raw_json": "{}",
    }])


@patch("wswdy.routes.public.geocode_address", new_callable=AsyncMock)
async def test_full_happy_path(mock_geo, app, tmp_path):
    fake_email = FakeNotifier()
    fake_wa = FakeNotifier()
    app.state.email_notifier = fake_email
    app.state.whatsapp_notifier = fake_wa
    # Refresh alerter to use the fake email
    from wswdy.alerts import AdminAlerter
    app.state.alerter = AdminAlerter(
        db=app.state.db, email=fake_email,
        admin_email="ian@test", ha_webhook_url="",
    )

    mock_geo.return_value = {"lat": 38.9097, "lon": -77.0319,
                              "display": "1500 14th St NW, Washington, DC"}

    client = TestClient(app)

    # 1. Get the signup form
    r = client.get("/")
    assert r.status_code == 200

    # 2. Submit signup
    r = client.post("/signup", data={
        "display_name": "Ian", "address_text": "1500 14th St NW",
        "preferred_channel": "email", "email": "ian@test",
        "radius_m": "1000",
    }, follow_redirects=False)
    assert r.status_code == 303

    # 3. Admin gets the review email
    review_emails = [e for e in fake_email.sent if "new signup" in e["subject"].lower()]
    assert review_emails
    review_text = review_emails[-1]["text"]
    # Extract the approve URL
    import re
    m = re.search(r"https://x\.test/a/([^\s]+)", review_text)
    assert m
    token = m.group(1)

    # 4. Approve
    r = client.post(f"/a/{token}/approve", follow_redirects=False)
    assert r.status_code == 303
    pending = subs_repo.list_by_status(app.state.db, "APPROVED")
    assert len(pending) == 1
    sid = pending[0]["id"]

    # 5. Welcome email landed
    welcome = [e for e in fake_email.sent if "welcome" in e["subject"].lower()
               or "you're confirmed" in e["text"].lower()]
    assert welcome

    # 6. Seed a crime in the radius
    _seed_recent_crime(app)

    # 7. Manually run the daily send (no scheduler dependency in test)
    from wswdy.jobs.send import run_daily_sends
    out = await run_daily_sends(
        db=app.state.db, email=fake_email, whatsapp=fake_wa,
        alerter=app.state.alerter,
        base_url="https://x.test", hmac_secret="secret",
        send_date=str(date.today()),
        now_iso=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        stagger=False, render_static_map=None,
    )
    assert out["sent"] == 1
    digests = [e for e in fake_email.sent
               if "DC briefing" in e["subject"] or "Good morning" in e["text"]]
    assert digests
    digest = digests[-1]
    assert "Ian" in digest["text"]
    assert "armed robbery" in digest["text"].lower()

    # 8. Visit the map
    from wswdy.tokens import sign
    map_token = sign("secret", purpose="map", subscriber_id=sid)
    r = client.get(f"/map/{sid}?token={map_token}")
    assert r.status_code == 200
    r = client.get(f"/api/crimes?subscriber={sid}&token={map_token}&window=24h")
    assert r.status_code == 200
    assert len(r.json()["features"]) == 1

    # 9. Hit /api/preview
    r = client.post("/api/preview",
                    json={"lat": 38.9097, "lon": -77.0319, "radius_m": 1000})
    assert r.status_code == 200

    # 10. Visit admin
    r = client.get("/admin?token=ADMINTOK")
    assert r.status_code == 200

    # 11. Unsubscribe
    unsub_token = sign("secret", purpose="unsubscribe", subscriber_id=sid)
    r = client.post(f"/u/{sid}?token={unsub_token}")
    assert r.status_code == 200
    s = subs_repo.get(app.state.db, sid)
    assert s["status"] == "UNSUBSCRIBED"
```

- [ ] **Step 2: Run; commit**

```bash
pytest tests/test_e2e_smoke.py -v
git add tests/test_e2e_smoke.py
git commit -m "test(e2e): full happy-path smoke test (signup → digest → unsubscribe)"
```

- [ ] **Step 3: Final test run + push**

```bash
pytest -v          # everything green
git push origin main
```

**End of Phase 9.** Project is complete. Deploy per `docs/deploy.md` and sign yourself up as the first subscriber.

---

## Notes for the implementer

- **Run tests after every task.** If they pass, commit. If they fail, the task isn't done.
- **One commit per task.** Don't lump multiple tasks together — small commits are easier to revert and review.
- **TDD discipline.** Don't skip the "run test, see failure" step before implementing. The fail message tells you the test reaches the right code.
- **No DB mocks.** All tests use a real in-memory or temp-file SQLite. Mocks are reserved for HTTP (respx) and outbound notifiers (FakeNotifier).
- **Async all the way down.** All notifier methods are `async`; FastAPI routes that touch them must be `async def`.
- **Don't bypass the repos.** No SQL outside `src/wswdy/repos/`. If you need a query the repo doesn't expose, add a method.
- **Prod env file is sacred.** Never commit `.env`. Always work from `.env.example`.

When ready, sign up at `https://dccrime.iandmuir.com/` as Ian, approve via the admin email, and verify the next morning's digest arrives.
