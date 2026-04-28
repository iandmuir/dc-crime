# DC Crime Alerts — Design

**Status:** Draft for review
**Author:** Ian Muir
**Date:** 2026-04-28

## Purpose

A small personal service that emails or WhatsApps a daily crime summary for the area around a subscriber's home address, sourced from the DC Metropolitan Police Department's public GeoJSON feed. Each message includes a link to an interactive map centered on the subscriber's address. Initial audience: friends and family (manual approval, <30 subscribers expected).

## Goals & non-goals

**Goals**
- Daily digest at ~06:00 ET summarizing reported crimes in the prior 24 hours within a user-chosen radius.
- Per-subscriber interactive map with 24h / 7d / 30d toggles.
- Single-vendor map stack (MapTiler) for tiles, geocoding, and static previews.
- Run entirely on Ian's NUC, exposed via Cloudflare Tunnel at `dccrime.iandmuir.com`.
- Resilient to expected failures (MPD feed down, WhatsApp session expired, SMTP outages) with admin alerts that don't depend on the failing component.

**Non-goals**
- Public, self-service onboarding (signup is invite-only via manual approval).
- Crime prediction, trend analysis, or commentary on incidents.
- Real-time alerts for crimes-in-progress.
- Coverage outside DC.
- High-availability / multi-region. One LXC, one process.

## High-level architecture

```
        dccrime.iandmuir.com
              │ (Cloudflare Tunnel)
              ▼
   ┌────────────────────────────────────────┐
   │ LXC: dc-crime-app (Python 3.12)        │
   │                                        │
   │  FastAPI (signup, confirm, map, API)   │
   │  APScheduler (cron jobs in-process)    │
   │  SQLite (WAL mode)                     │
   └────────────────────────────────────────┘
              │
   ┌──────────┼──────────────────────────┐
   ▼          ▼                          ▼
 MPD     MapTiler                 WhatsApp MCP
 feed   (tiles+geo+               (existing LXC,
        static)                    secondary number
                                   +1 202 468 2709)
              │
              ▼
        SMTP / Home Assistant webhook (admin alerts)
```

**Single-process design.** FastAPI and APScheduler run in the same Python process. No queue, no Redis, no Docker compose. Persistence is one SQLite file. The WhatsApp MCP stays in its existing LXC and is reached over HTTP.

## Tech stack

- **Language/runtime:** Python 3.12
- **Web framework:** FastAPI + Uvicorn
- **Scheduler:** APScheduler (in-process, persistent job store backed by SQLite)
- **Database:** SQLite with WAL mode
- **Templating:** Jinja2 (HTML pages, email bodies, message text)
- **Geometry:** `shapely` (haversine via `geopy` is fine; SQL haversine is sufficient at this scale)
- **HTTP client:** `httpx`
- **Map (browser):** Leaflet with MapTiler raster tiles
- **Static map preview:** MapTiler Static Maps API
- **Geocoding:** MapTiler Geocoding API
- **Email:** `aiosmtplib` against an SMTP provider (provider TBD during implementation; Fastmail or Resend are both fine)
- **WhatsApp:** existing MCP bridge (re-pointed at +1 202 468 2709), called over HTTP
- **Process supervision:** `systemd` unit inside the LXC

## Subscriber lifecycle

```
   [signup form submitted]
            │
            ▼
   status = PENDING
            │
            ├─► email to iandmuir@gmail.com with [Approve] [Reject] links
            │   (HMAC-signed, 7-day expiry)
            │
            ▼
   admin clicks Approve
            │
            ▼
   status = APPROVED
            │
            ├─► one-time welcome message via chosen channel
            │   ("you're confirmed; first digest tomorrow at 6am")
            │
            ▼
   subscriber receives daily digests
            │
            ▼
   user clicks unsubscribe link
            │
            ▼
   status = UNSUBSCRIBED   (soft delete; record retained)
```

**Statuses:** `PENDING`, `APPROVED`, `REJECTED`, `UNSUBSCRIBED`.

**Approval link expiry:** 7 days. Expired requests can be re-submitted via the form.

**Welcome message** is a real send through the chosen channel. It serves as a smoke test — if it fails, the admin is alerted immediately so the channel can be fixed before the first digest.

## Data model

```sql
CREATE TABLE subscribers (
  id              TEXT PRIMARY KEY,           -- short random id, used in URLs
  display_name    TEXT NOT NULL,              -- e.g. "Jane"
  email           TEXT,                       -- one of email/phone required
  phone           TEXT,                       -- E.164, e.g. +14155551234
  preferred_channel TEXT NOT NULL CHECK(preferred_channel IN ('email','whatsapp')),
  address_text    TEXT NOT NULL,              -- as entered
  lat             REAL NOT NULL,
  lon             REAL NOT NULL,
  radius_m        INTEGER NOT NULL,           -- 100..2000
  status          TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING|APPROVED|REJECTED|UNSUBSCRIBED
  created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  approved_at     TIMESTAMP,
  unsubscribed_at TIMESTAMP,
  last_sent_at    TIMESTAMP
);

CREATE INDEX subscribers_status_idx ON subscribers(status);

CREATE TABLE crimes (
  ccn            TEXT PRIMARY KEY,            -- MPD case number, unique
  offense        TEXT NOT NULL,
  method         TEXT,                        -- GUN | KNIFE | OTHERS | null
  shift          TEXT,                        -- DAY | EVENING | MIDNIGHT
  block_address  TEXT,                        -- redacted street block
  lat            REAL NOT NULL,
  lon            REAL NOT NULL,
  report_dt      TIMESTAMP NOT NULL,          -- REPORT_DAT in feed
  start_dt       TIMESTAMP,                   -- START_DATE in feed
  end_dt         TIMESTAMP,                   -- END_DATE in feed (often null)
  ward           TEXT,
  district       TEXT,
  raw_json       TEXT,                        -- full feed properties for forensic use
  fetched_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX crimes_report_dt_idx ON crimes(report_dt);
CREATE INDEX crimes_geo_idx ON crimes(lat, lon);

CREATE TABLE send_log (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  subscriber_id  TEXT NOT NULL REFERENCES subscribers(id),
  send_date      DATE NOT NULL,               -- the digest date (today's date at 06:00 ET)
  channel        TEXT NOT NULL,               -- email | whatsapp
  status         TEXT NOT NULL,               -- sent | failed | skipped
  error          TEXT,
  sent_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(subscriber_id, send_date, channel)   -- idempotency
);

CREATE TABLE fetch_log (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  fetched_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status         TEXT NOT NULL,
  crimes_added   INTEGER,
  crimes_updated INTEGER,
  error          TEXT
);

CREATE TABLE admin_alerts (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_type     TEXT NOT NULL,
  message        TEXT NOT NULL,
  created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  suppressed_until TIMESTAMP                   -- for rate-limiting repeat alerts
);
```

**Tokens are not stored.** All tokens (approve, unsubscribe, map view, admin) are HMAC-signed payloads using a server-side secret — the server can validate without a DB lookup. Payload format: `{purpose, subscriber_id, expires_at|null}` → base64url(payload) + `.` + base64url(hmac).

## Severity tier rules

| Tier | Color | Offenses |
|------|-------|----------|
| 1 | 🔴 Dark red | Homicide, Sex Abuse, Assault w/ Dangerous Weapon, **Armed Robbery** (Robbery where `METHOD` ∈ {GUN, KNIFE}) |
| 2 | 🟠 Orange | Robbery (unarmed), Burglary, Arson |
| 3 | 🟡 Yellow | Motor Vehicle Theft |
| 4 | 🟢 Green | Theft from Auto, Theft/Other |

Implemented as a single `classify(offense, method) -> tier` pure function with a unit test per branch.

## Daily message format

WhatsApp version (email is the same content with HTML styling and the static map embedded inline):

```
Good morning Jane ☀️

In the last 24 hours there were 7 crimes reported within 800m of your home:

🔴 1 violent  — 1 armed robbery
🟠 0 serious property
🟡 1 vehicle  — 1 motor vehicle theft
🟢 5 petty    — 3 theft from auto, 2 other theft

Closest to you:
• Armed robbery — 280m away (1400 block of P St NW, 21:14)
• Theft from auto — 190m away (1500 block of 14th St NW, 02:30)

📍 View map (last 24h, with toggles for 7d / 30d):
https://dccrime.iandmuir.com/map/abc123?token=xyz

Reply STOP or click to unsubscribe:
https://dccrime.iandmuir.com/u/abc123?token=xyz
```

**Rules:**
- Tier counts always shown, including zeros.
- "Closest to you" lists up to 3 incidents within `radius_m / 2`. On a clean day, replaced with `No incidents reported in your immediate vicinity. ✨`.
- Times rendered in ET, 24-hour.
- Block-level addresses only (matches MPD's published data).
- WhatsApp messages send the static map preview as an image attachment with the text as caption.
- Email messages embed the static map inline; the same interactive link is offered.

**Edge cases:**
- **Zero crimes in 24h:** still send a message ("Quiet night — no incidents reported in your area. View past week →"). Reassurance is the product.
- **>50 crimes:** same format, "closest to you" remains capped at 3, the map shows the rest.
- **MPD feed unavailable at send time:** message says "MPD data temporarily unavailable — we'll catch you up tomorrow." Admin alerted.

## Pages & API surface

**Public pages (HTML):**
- `GET  /`                              — signup form
- `POST /signup`                        — submit signup
- `GET  /a/{token}`                     — admin approval/rejection landing
- `POST /a/{token}/approve|reject`      — admin action
- `GET  /map/{subscriber_id}?token=…`   — interactive map
- `GET  /u/{subscriber_id}?token=…`     — unsubscribe confirmation page
- `POST /u/{subscriber_id}?token=…`     — perform unsubscribe
- `GET  /admin?token=…`                 — read-only admin dashboard

**JSON API (consumed by the map page):**
- `GET /api/crimes?subscriber={id}&token=…&window={24h|7d|30d}` — returns GeoJSON FeatureCollection of crimes matching the subscriber's center/radius and the chosen window. Tier added as a property on each feature.

**Health:**
- `GET /healthz` — internal LXC use only (not exposed via Cloudflare Tunnel)

## Frontend pages requiring design

Three pages will be built via the `frontend-design` skill *before* backend implementation begins, designed as a coherent set:

1. **Signup form** (`/`) — fields: display name, address (with MapTiler autocomplete), email, phone, preferred channel toggle, radius slider (100–2000m, default 800m), submit. Inline validation. Address-not-in-DC error.
2. **Unsubscribe confirmation** (`/u/{id}`) — pre-fill subscriber's name, "Are you sure?" → confirm button → success state with a "Re-subscribe" link back to the signup form.
3. **Map page** (`/map/{id}`) — Leaflet map filling viewport, recipient's name + radius shown unobtrusively, a 24h / 7d / 30d toggle, color-coded markers per tier, click-to-popup with offense + block + time, legend.

The admin dashboard (`/admin`) is functional-only — no design pass needed.

## Operations

### Cron schedule (all times ET)

| Time | Job | Notes |
|------|-----|-------|
| 03:00 | Prune crimes older than 90 days | Keeps DB small |
| 05:30 | Fetch MPD feed (last 31 days) | Upsert by CCN; 3 retries with 5/15/45 min backoff |
| 06:00 | Send daily digests | Stagger over 0–45 min, 30–120s random gap per send |
| 23:00 | Health snapshot email to admin | "Today: fetched X, sent Y, Z failures" |

### Idempotency

- `send_log` has `UNIQUE(subscriber_id, send_date, channel)`. A restarted send job skips already-completed recipients.
- Crime upserts are keyed on CCN; re-fetching the same window is a no-op.

### Failure handling

| Failure | Behavior |
|---------|----------|
| MPD feed unavailable at 05:30 | Retry 3x with backoff. If still failing by 06:30, send digests using last successful fetch and append `⚠️ MPD data may be delayed.` to messages. Admin alerted. |
| WhatsApp MCP unreachable | Per-subscriber: 2 retries with backoff, then fall back to email if available. If only WhatsApp is on file, skip and add to admin failure list. |
| WhatsApp session expired | Detected by characteristic error from MCP; admin alerted immediately so device can be re-linked. |
| SMTP unavailable | Queue and retry every 30 min for up to 6 hours. Beyond that, drop and log. |
| Geocoding fails on signup | Form returns inline error. No PENDING record created. |

### Admin alerting

Each admin alert is sent through **two independent channels**:

1. **Email** to `iandmuir@gmail.com` (separate dependency chain from WhatsApp, so it's the right fallback when WhatsApp is the failing component).
2. **Home Assistant webhook** — POST to a configured HA webhook URL, which fires whatever notification flow Ian has already set up in HA (mobile push, persistent banner, etc.).

Repeat alerts of the same `alert_type` are suppressed for 6 hours via `admin_alerts.suppressed_until` to prevent alert storms.

### Observability

- One log file per day: `/var/log/dccrime/YYYY-MM-DD.log`. 30-day retention via logrotate.
- Daily 23:00 health snapshot email is the primary dashboard.
- `/admin?token=…` shows: subscriber counts by status, last fetch time + count, last 7 days of send volume, last 20 errors. Read-only.
- No Prometheus/Grafana — explicitly out of scope.

### Backups

- Daily tarball of `dccrime.db` + `.env` (encrypted) uploaded to a destination chosen at deploy time. Default plan: rclone to a Google Drive folder. 14-day retention.

### Secrets

- `.env` in the LXC, gitignored, world-readable only by the service user.
- Holds: `MAPTILER_API_KEY`, SMTP credentials, MCP endpoint URL + auth, `HMAC_SECRET`, `ADMIN_TOKEN`, `HA_WEBHOOK_URL`.

### Local development

- `make dev` runs Uvicorn with hot-reload against a local SQLite file and a fixture MPD JSON file, so iteration doesn't burn MapTiler quota or hammer the live feed.
- `make seed` populates the local DB with a few synthetic subscribers and crimes for UI testing.
- A `pytest` suite covers tier classification, distance/radius queries, token signing/verification, and the `Notifier` interface (with a fake notifier for tests).

## Notifier abstraction

A small interface so the channel choice is swappable later (e.g., migrate WhatsApp to the official Cloud API):

```python
class Notifier(Protocol):
    async def send(self, recipient: str, subject: str, text: str,
                   image_path: Path | None) -> SendResult: ...
```

Concrete implementations: `EmailNotifier`, `WhatsAppMcpNotifier`, `FakeNotifier` (tests).

Routing layer: `dispatch(subscriber, payload)` selects the implementation based on `subscriber.preferred_channel`, with email as the fallback when WhatsApp fails and email is on file.

## Privacy & security

- Address text + resolved lat/lon stored in plaintext SQLite (acceptable at friends-and-family scale, gated by SQLite file permissions and LXC isolation).
- All public links are HMAC-signed against a server-side secret.
- Map-view tokens have **no expiry** — leaking a URL only reveals the public crime map centered on a roughly-known area, which is low-sensitivity.
- Unsubscribe tokens have **no expiry** (good-citizen: unsubscribe must always work).
- Approve/reject tokens expire after 7 days.
- Form submissions are rate-limited per source IP (10 / hour) to deter abuse via the public form.
- No PII appears in log files (subscriber IDs are logged; names, emails, phone numbers are not).

## Risks & open items

| Risk | Mitigation |
|------|-----------|
| WhatsApp account ban on the secondary number | Stagger sends, personalized content, recipients save the number to contacts before first send. If banned, switch the MCP bridge to a fresh secondary number with no migration needed beyond config. |
| MapTiler quota exhaustion | Free tier is generous; the static map preview is the heavy hitter. Cache static previews per subscriber per day (one render per subscriber per send). |
| MPD schema change | `raw_json` column preserves the full feed properties so we can reprocess if a field renames. |
| Cloudflare Tunnel down | Public surface is unreachable, but the daily send still runs (it doesn't depend on inbound HTTP). Map links will fail until the tunnel is restored. |
| Single-host failure (NUC down) | Out of scope. Acceptable at this scale. |

## Out-of-scope (future)

- Migration to WhatsApp Cloud API (keep the Notifier interface clean so this is a 1-day swap).
- SMS channel.
- Multi-city support (would require generalizing the MPD-specific pieces).
- Self-service approval (drop the manual-approval gate when scale demands it).
- Per-user notification preferences (e.g., "skip messages on quiet days").
- Trend/sparkline content in the daily message.

## Implementation phases

1. **Frontend mockups** — `frontend-design` produces the signup, unsubscribe, and map pages as a coherent visual set.
2. **Backend implementation plan** — `writing-plans` produces a step-by-step implementation plan covering: DB layer, MPD fetcher + classifier, Notifier abstraction + implementations, FastAPI routes, scheduler, admin tooling, deployment to LXC.
3. **Build & deploy** — execute the plan, deploy behind Cloudflare Tunnel, smoke-test with Ian as the first subscriber.
