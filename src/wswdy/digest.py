"""Digest message builder — produces the WhatsApp/email body text."""
from datetime import datetime
from zoneinfo import ZoneInfo

from wswdy.address import humanize_address
from wswdy.geo import haversine_m
from wswdy.offenses import humanize_offense
from wswdy.tiers import classify, classify_arrest, classify_crash

ET = ZoneInfo("America/New_York")

_TIER_GLYPH = {1: "🔴", 2: "🟠", 3: "🟡", 4: "🟢"}
_TIER_LABEL = {1: "violent", 2: "serious property", 3: "vehicle", 4: "petty"}

# Crash tier glyphs use a different palette so subscribers can tell at a
# glance which section they're reading. Black for fatalities, then descending
# severity in red/orange/grey.
_CRASH_TIER_GLYPH = {1: "⚫", 2: "🔴", 3: "🟠", 4: "⚪"}
_CRASH_TIER_LABEL = {
    1: "fatal", 2: "major injuries", 3: "minor injuries", 4: "property damage",
}

# Arrests have only 3 tiers (felony / misdemeanor / unspecified).
# Glyphs mirror the crime palette so subscribers don't have to learn a
# third color scheme — same red/yellow/grey severity ramp.
_ARREST_TIER_GLYPH = {1: "🔴", 2: "🟡", 3: "⚪"}
_ARREST_TIER_LABEL_SINGULAR = {
    1: "felony", 2: "misdemeanor", 3: "unspecified",
}
_ARREST_TIER_LABEL_PLURAL = {
    1: "felonies", 2: "misdemeanors", 3: "unspecified",
}


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
    """Render ISO UTC string as 24h ET time string."""
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(ET)
    return dt.strftime("%H:%M")


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
    crashes: list[dict], radius_str: str, *, new_count: int = 0,
) -> list[str]:
    """Render the crashes block. Returns [] if we should omit the section
    (we render even when zero so it reads like a quiet weather report).

    ``new_count`` is the number of crashes added to our DB since the
    previous daily fetch. Surfaced inline in the header because crash
    data has a 3-5 day publishing lag — without this number, every day's
    digest looks identical and subscribers can't tell when DC has
    published a fresh batch.
    """
    n = len(crashes)
    lines: list[str] = []
    lines.append("")
    if n == 0:
        lines.append(
            f"🚦 No crashes reported within {radius_str} in the last 7 days."
        )
        return lines

    counts = _summarize_crashes_by_tier(crashes)
    suffix = f", {new_count} newly reported" if new_count > 0 else ""
    lines.append(f"🚦 Crashes within {radius_str} (last 7 days{suffix}):")
    for tier in (1, 2, 3, 4):
        c = counts[tier]
        if c == 0:
            continue
        lines.append(f"{_CRASH_TIER_GLYPH[tier]} {c} {_CRASH_TIER_LABEL[tier]}")

    callouts = _crash_callout_lines(crashes)
    if callouts:
        lines.append("")
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
    """Format one arrest into the multi-line 'closest' callout block."""
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
    distance = f" · {a['distance_m']}m away" if "distance_m" in a else ""
    return f"• {label} — {offense}{demo}\n   📍 {addr}{distance}{when}"


def _arrest_section_lines(
    arrests: list[dict],
    radius_str: str,
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
      - Report in but zero arrests in radius → "No arrests within Xm..."
      - Report in with arrests → tier counts + the 2 closest with name
        / age / gender / address / time.
    """
    lines: list[str] = ["", ]
    if not have_arrest_today:
        lines.append(
            f"👮 No arrest report yet today — usually arrives mid-morning."
        )
        return lines
    if not arrests:
        lines.append(
            f"👮 No arrests within {radius_str} in the last 24h."
        )
        return lines

    counts = _summarize_arrests_by_tier(arrests)
    lines.append(f"👮 Arrests within {radius_str} (last 24h):")
    for tier in (1, 2, 3):
        c = counts[tier]
        if c == 0:
            continue
        label = (_ARREST_TIER_LABEL_PLURAL if c != 1
                 else _ARREST_TIER_LABEL_SINGULAR)[tier]
        lines.append(f"{_ARREST_TIER_GLYPH[tier]} {c} {label}")

    closest = _select_closest_arrests(
        arrests, home_lat=home_lat, home_lon=home_lon,
        radius_m=radius_m, max_items=2,
    )
    if closest:
        lines.append("")
        lines.append("Closest:")
        for a in closest:
            lines.append(_arrest_callout(a))
    return lines


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
) -> str:
    """Build the full digest message body."""
    n = len(crimes)
    counts = summarize_by_tier(crimes)
    radius_str = f"{radius_m:,}m"

    lines: list[str] = []
    lines.append(f"Good morning, {display_name} ☀️")
    lines.append("")
    if n == 0:
        lines.append(
            f"Quiet night — 0 crimes reported within {radius_str} of your home in the last 24h."
        )
    else:
        lines.append(
            f"In the last 24 hours there were {n} crimes reported within {radius_str} of your home:"
        )
        lines.append("")
        for tier in (1, 2, 3, 4):
            c = counts[tier]
            label = _TIER_LABEL[tier]
            glyph = _TIER_GLYPH[tier]
            examples = _tier_examples(crimes, tier)
            if c == 0:
                lines.append(f"{glyph} 0 {label}")
            elif examples:
                lines.append(f"{glyph} {c} {label}  — {examples}")
            else:
                lines.append(f"{glyph} {c} {label}")

    lines.append("")
    closest = select_closest(crimes, home_lat=home_lat, home_lon=home_lon,
                             radius_m=radius_m, max_items=3)
    if closest:
        lines.append("Closest to you:")
        for c in closest:
            offense = _humanize_offense(c["offense"], c.get("method"))
            t = _fmt_time(c["report_dt"])
            lines.append(
                f"• {offense} — {c['distance_m']}m away "
                f"({humanize_address(c['block_address'])}, {t})"
            )
    else:
        lines.append("No incidents reported in your immediate vicinity. ✨")

    # Crashes section — rolling 7-day window. We show it even when zero
    # because the absence is reassuring (and the section's existence is
    # data — readers know the feed was checked).
    if crashes is not None:
        lines.extend(_crash_section_lines(
            crashes, radius_str, new_count=new_crash_count,
        ))

    # Arrests section. We render it whenever the caller passed an
    # ``arrests`` list — even an empty list is signal (the report
    # arrived, no arrests near you). The ``have_arrest_today`` flag
    # disambiguates "0 because nothing happened" from "0 because the
    # email hasn't arrived yet".
    if arrests is not None:
        lines.extend(_arrest_section_lines(
            arrests, radius_str,
            have_arrest_today=have_arrest_today,
            home_lat=home_lat, home_lon=home_lon, radius_m=radius_m,
        ))

    lines.append("")
    lines.append(f"🗺️ Map: {map_url}")
    lines.append("")
    lines.append("Reply STOP to unsubscribe.")

    if mpd_warning:
        lines.append("")
        lines.append("⚠️ MPD data may be delayed — we'll catch you up tomorrow.")

    return "\n".join(lines)
