"""Digest message builder — produces the WhatsApp/email body text.

Layout principles (2026-05 redesign):

- Inverted pyramid: a one-line TL;DR right under the header answers "do I
  need to care today?" before any detail.
- One severity color ramp everywhere: red → orange → yellow → green, with
  black (⚫) reserved for fatalities in any section. Subscribers learn one
  legend, not three.
- Zero-count tier lines are folded into the summary instead of rendered
  ("none violent" beats four "0 …" rows).
- WhatsApp formatting: ``*bold*`` section headers give the message visual
  anchors. Email strips/re-renders via its own HTML template.
- Blocks + 12-hour times ("~2 blocks", "3:12 AM") instead of meters and
  24h clock — matches how DC readers think.
- The radius is stated once, in the Crimes header; other sections say
  "your zone".
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from wswdy.address import humanize_address
from wswdy.geo import haversine_m
from wswdy.offenses import humanize_offense
from wswdy.tiers import classify, classify_arrest, classify_crash

ET = ZoneInfo("America/New_York")

_TIER_GLYPH = {1: "🔴", 2: "🟠", 3: "🟡", 4: "🟢"}
_TIER_LABEL = {1: "violent", 2: "serious property", 3: "vehicle", 4: "petty"}

# Crash tiers reuse the same ramp; black = fatal.
_CRASH_TIER_GLYPH = {1: "⚫", 2: "🔴", 3: "🟡", 4: "🟢"}
_CRASH_TIER_LABEL = {
    1: "fatal", 2: "major injuries", 3: "minor injuries", 4: "property damage",
}

# Arrests: felony red, misdemeanor yellow, unspecified grey.
_ARREST_TIER_GLYPH = {1: "🔴", 2: "🟡", 3: "⚪"}
_ARREST_TIER_LABEL_SINGULAR = {
    1: "felony", 2: "misdemeanor", 3: "unspecified",
}
_ARREST_TIER_LABEL_PLURAL = {
    1: "felonies", 2: "misdemeanors", 3: "unspecified",
}

# One DC block is roughly 150m — good enough for "how far away was that?"
_M_PER_BLOCK = 150


def summarize_by_tier(crimes: list[dict]) -> dict[int, int]:
    """Count crimes by severity tier."""
    counts = {1: 0, 2: 0, 3: 0, 4: 0}
    for c in crimes:
        counts[classify(c["offense"], c.get("method"))] += 1
    return counts


def select_closest(crimes: list[dict], *, home_lat: float, home_lon: float,
                   radius_m: int, max_items: int = 3) -> list[dict]:
    """Return up to max_items crimes within half the radius, sorted by distance."""
    near_threshold = radius_m / 2
    enriched = []
    for c in crimes:
        d = haversine_m(home_lat, home_lon, c["lat"], c["lon"])
        if d <= near_threshold:
            enriched.append({**c, "distance_m": int(round(d))})
    enriched.sort(key=lambda x: x["distance_m"])
    return enriched[:max_items]


def _fmt_time(iso: str) -> str:
    """Render ISO UTC string as 12-hour ET time, e.g. '3:12 AM'."""
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(ET)
    return dt.strftime("%I:%M %p").lstrip("0")


def _fmt_blocks(distance_m: int) -> str:
    """Distance as approximate city blocks: '~1 block', '~4 blocks'."""
    n = max(1, round(distance_m / _M_PER_BLOCK))
    return f"~{n} block" + ("" if n == 1 else "s")


def _default_date_label() -> str:
    """Today in ET as e.g. 'Thu, May 7'. Manual day formatting because
    %-d is Linux-only and the dev box is Windows."""
    now = datetime.now(ET)
    return f"{now.strftime('%a, %b')} {now.day}"


def _humanize_offense(offense: str, method: str | None) -> str:
    """Backwards-compatible shim for callers in this module; the canonical
    implementation lives in wswdy.offenses."""
    return humanize_offense(offense, method)


def _tier_examples(crimes: list[dict], tier: int) -> str:
    """Brief example list for a tier, e.g. '1 armed robbery, 2 burglary'."""
    by_offense: dict[str, int] = {}
    for c in crimes:
        if classify(c["offense"], c.get("method")) != tier:
            continue
        label = _humanize_offense(c["offense"], c.get("method")).lower()
        by_offense[label] = by_offense.get(label, 0) + 1
    parts = [f"{n} {label}" for label, n in sorted(by_offense.items(), key=lambda x: -x[1])]
    return ", ".join(parts)


def _summarize_crashes_by_tier(crashes: list[dict]) -> dict[int, int]:
    counts = {1: 0, 2: 0, 3: 0, 4: 0}
    for c in crashes:
        counts[classify_crash(c)] += 1
    return counts


def _summary_line(
    *,
    crimes: list[dict],
    crashes: list[dict] | None,
    new_crash_count: int,
    arrests: list[dict] | None,
    have_arrest_today: bool,
) -> str:
    """The TL;DR line under the header. One glance = the whole story."""
    counts = summarize_by_tier(crimes)
    n = len(crimes)
    if n == 0:
        parts = ["No crimes near you"]
    else:
        v = counts[1]
        violent_desc = "none violent" if v == 0 else \
            f"{v} violent ⚠️"
        parts = [f"{n} crime{'s' if n != 1 else ''} ({violent_desc})"]

    if arrests is not None and have_arrest_today:
        na = len(arrests)
        if na:
            parts.append(f"{na} arrest{'s' if na != 1 else ''}")
        else:
            parts.append("no arrests")

    if crashes is not None:
        if new_crash_count > 0:
            parts.append(f"{new_crash_count} new crash"
                         f"{'es' if new_crash_count != 1 else ''}")
        else:
            parts.append("no new crashes")

    return " · ".join(parts)


def _crash_callout_lines(crashes: list[dict]) -> list[str]:
    """Pull out fatal + ped/cyclist-major-injury crashes for inline mentions.
    These are the highest-stakes crashes for a neighborhood newsletter."""
    callouts: list[str] = []
    for c in crashes:
        if (c.get("fatal") or 0) > 0:
            callouts.append(f"⚫ Fatal crash — {c.get('address') or 'address unknown'}")
        elif (c.get("ped_major") or 0) > 0:
            callouts.append(
                f"🔴 Pedestrian struck — {c.get('address') or 'address unknown'}"
            )
        elif (c.get("bike_major") or 0) > 0:
            callouts.append(
                f"🔴 Cyclist struck — {c.get('address') or 'address unknown'}"
            )
    return callouts[:3]  # cap so the digest doesn't balloon


def _crash_section_lines(
    crashes: list[dict], *, new_count: int = 0,
) -> list[str]:
    """Render the crashes block. We render even when zero so it reads like
    a quiet weather report.

    ``new_count`` (crashes added to our DB since the previous daily fetch)
    leads the section when non-zero: crash data has a 3-5 day publishing
    lag, so "what just got published" is the only thing that changes
    day-to-day.
    """
    n = len(crashes)
    lines: list[str] = [""]
    if n == 0:
        lines.append("🚦 No crashes near you in the last 7 days.")
        return lines

    lines.append("*Crashes* — last 7 days")
    if new_count > 0:
        lines.append(f"{new_count} newly reported this week.")
    counts = _summarize_crashes_by_tier(crashes)
    tier_bits = [
        f"{_CRASH_TIER_GLYPH[t]} {counts[t]} {_CRASH_TIER_LABEL[t]}"
        for t in (1, 2, 3, 4) if counts[t]
    ]
    if tier_bits:
        lines.append(" · ".join(tier_bits))

    callouts = _crash_callout_lines(crashes)
    if callouts:
        lines.extend(callouts)
    return lines


# ---------- arrest section ------------------------------------------------

def _summarize_arrests_by_tier(arrests: list[dict]) -> dict[int, int]:
    counts = {1: 0, 2: 0, 3: 0}
    for a in arrests:
        counts[classify_arrest(a)] += 1
    return counts


def _arrest_full_name(a: dict) -> str:
    parts = []
    for k in ("offender_first", "offender_last"):
        v = (a.get(k) or "").strip().title()
        if v:
            parts.append(v)
    return " ".join(parts)


def _select_closest_arrests(
    arrests: list[dict], *, home_lat: float, home_lon: float,
    radius_m: int, max_items: int = 2,
) -> list[dict]:
    """Mirror of ``select_closest`` but for arrests. Same half-radius
    threshold and distance sort."""
    near_threshold = radius_m / 2
    enriched = []
    for a in arrests:
        if a.get("lat") is None or a.get("lon") is None:
            continue
        d = haversine_m(home_lat, home_lon, a["lat"], a["lon"])
        if d <= near_threshold:
            enriched.append({**a, "distance_m": int(round(d))})
    enriched.sort(key=lambda x: x["distance_m"])
    return enriched[:max_items]


def _arrest_callout(a: dict) -> str:
    """Format one arrest into the multi-line 'nearest' callout block."""
    tier = classify_arrest(a)
    label = _ARREST_TIER_LABEL_SINGULAR[tier].capitalize()
    offense = (a.get("offense") or "Unknown offense").strip()
    name = _arrest_full_name(a)
    age = a.get("age")
    gender = (a.get("gender") or "").strip()
    demo_bits = []
    if age:
        demo_bits.append(str(age))
    if gender:
        demo_bits.append(gender[0].upper())
    demo = f" ({name}, {', '.join(demo_bits)})" if name and demo_bits \
        else f" ({name})" if name \
        else ""
    addr = humanize_address(a.get("arrest_location") or "")
    when_t = _fmt_time(a["arrest_dt"]) if a.get("arrest_dt") else ""
    when = f" · {when_t}" if when_t else ""
    distance = f" · {_fmt_blocks(a['distance_m'])}" if "distance_m" in a else ""
    return f"• {label} — {offense}{demo}\n   📍 {addr}{distance}{when}"


def _arrest_section_lines(
    arrests: list[dict],
    *,
    have_arrest_today: bool,
    home_lat: float,
    home_lon: float,
    radius_m: int,
) -> list[str]:
    """Render the arrests block.

    Three rendering paths:

      - No report ingested today yet → "No arrest report yet for ..."
        (so subscribers can tell missing data apart from a quiet day).
      - Report in but zero arrests in radius → "No arrests within ..."
      - Report in with arrests → compact tier counts + the 2 nearest with
        name / age / gender / address / time.
    """
    lines: list[str] = ["", ]
    if not have_arrest_today:
        lines.append(
            "👮 No arrest report yet today — usually arrives mid-morning."
        )
        return lines
    if not arrests:
        lines.append("👮 No arrests within your zone in the last 24h.")
        return lines

    counts = _summarize_arrests_by_tier(arrests)
    lines.append("*Arrests* — last 24h")
    tier_bits = []
    for tier in (1, 2, 3):
        c = counts[tier]
        if c == 0:
            continue
        label = (_ARREST_TIER_LABEL_PLURAL if c != 1
                 else _ARREST_TIER_LABEL_SINGULAR)[tier]
        tier_bits.append(f"{_ARREST_TIER_GLYPH[tier]} {c} {label}")
    if tier_bits:
        lines.append(" · ".join(tier_bits))

    closest = _select_closest_arrests(
        arrests, home_lat=home_lat, home_lon=home_lon,
        radius_m=radius_m, max_items=2,
    )
    if closest:
        lines.append("")
        lines.append("Nearest:")
        for a in closest:
            lines.append(_arrest_callout(a))
    return lines


def build_digest_subject(
    *,
    date_short: str,
    crimes: list[dict],
    arrests: list[dict] | None = None,
    have_arrest_today: bool = False,
) -> str:
    """Informative email subject so subscribers can triage from the inbox:
    'WTFDC 5/7 — 4 crimes (none violent), 1 arrest nearby'."""
    n = len(crimes)
    if n == 0:
        crime_part = "quiet night"
    else:
        v = summarize_by_tier(crimes)[1]
        violent = "none violent" if v == 0 else f"{v} violent"
        crime_part = f"{n} crime{'s' if n != 1 else ''} ({violent})"
    parts = [crime_part]
    if arrests is not None and have_arrest_today and arrests:
        na = len(arrests)
        parts.append(f"{na} arrest{'s' if na != 1 else ''} nearby")
    return f"WTFDC {date_short} — {', '.join(parts)}"


def build_digest_text(
    *,
    display_name: str,
    radius_m: int,
    crimes: list[dict],
    home_lat: float,
    home_lon: float,
    map_url: str,
    unsubscribe_url: str,
    crashes: list[dict] | None = None,
    new_crash_count: int = 0,
    arrests: list[dict] | None = None,
    have_arrest_today: bool = False,
    mpd_warning: bool = False,
    date_label: str | None = None,
) -> str:
    """Build the full digest message body (WhatsApp formatting)."""
    n = len(crimes)
    counts = summarize_by_tier(crimes)
    radius_str = f"{radius_m:,}m"
    if date_label is None:
        date_label = _default_date_label()

    lines: list[str] = []
    lines.append(f"*WTFDC for {display_name} — {date_label}* ☀️")
    lines.append(_summary_line(
        crimes=crimes, crashes=crashes, new_crash_count=new_crash_count,
        arrests=arrests, have_arrest_today=have_arrest_today,
    ))
    lines.append("")

    if n == 0:
        lines.append(
            f"Quiet night — 0 crimes reported within {radius_str} of your home in the last 24h."
        )
    else:
        lines.append(f"*Crimes* — last 24h, your {radius_str} zone")
        for tier in (1, 2, 3, 4):
            c = counts[tier]
            if c == 0:
                continue
            label = _TIER_LABEL[tier]
            glyph = _TIER_GLYPH[tier]
            examples = _tier_examples(crimes, tier)
            if examples:
                lines.append(f"{glyph} {c} {label} — {examples}")
            else:
                lines.append(f"{glyph} {c} {label}")

    closest = select_closest(crimes, home_lat=home_lat, home_lon=home_lon,
                             radius_m=radius_m, max_items=3)
    if closest:
        lines.append("")
        lines.append("Nearest to you:")
        for c in closest:
            offense = _humanize_offense(c["offense"], c.get("method"))
            t = _fmt_time(c["report_dt"])
            lines.append(
                f"• {offense} — {_fmt_blocks(c['distance_m'])} "
                f"({humanize_address(c['block_address'])}, {t})"
            )
    elif n > 0:
        lines.append("")
        lines.append("No incidents reported in your immediate vicinity. ✨")

    # Crashes section — rolling 7-day window. We show it even when zero
    # because the absence is reassuring (and the section's existence is
    # data — readers know the feed was checked).
    if crashes is not None:
        lines.extend(_crash_section_lines(crashes, new_count=new_crash_count))

    # Arrests section. We render it whenever the caller passed an
    # ``arrests`` list — even an empty list is signal (the report
    # arrived, no arrests near you). The ``have_arrest_today`` flag
    # disambiguates "0 because nothing happened" from "0 because the
    # email hasn't arrived yet".
    if arrests is not None:
        lines.extend(_arrest_section_lines(
            arrests,
            have_arrest_today=have_arrest_today,
            home_lat=home_lat, home_lon=home_lon, radius_m=radius_m,
        ))

    lines.append("")
    lines.append(f"🗺️ Map: {map_url}")
    lines.append("Reply STOP to unsubscribe.")

    if mpd_warning:
        lines.append("")
        lines.append("⚠️ MPD data may be delayed — we'll catch you up tomorrow.")

    return "\n".join(lines)
