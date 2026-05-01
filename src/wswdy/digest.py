"""Digest message builder — produces the WhatsApp/email body text."""
from datetime import datetime
from zoneinfo import ZoneInfo

from wswdy.address import humanize_address
from wswdy.geo import haversine_m
from wswdy.offenses import humanize_offense
from wswdy.tiers import classify, classify_crash

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


def _crash_section_lines(crashes: list[dict], radius_str: str) -> list[str]:
    """Render the crashes block. Returns [] if we should omit the section
    (we render even when zero so it reads like a quiet weather report)."""
    n = len(crashes)
    lines: list[str] = []
    lines.append("")
    if n == 0:
        lines.append(
            f"🚦 No crashes reported within {radius_str} in the last 7 days."
        )
        return lines

    counts = _summarize_crashes_by_tier(crashes)
    lines.append(f"🚦 Crashes within {radius_str} (last 7 days):")
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
        lines.extend(_crash_section_lines(crashes, radius_str))

    lines.append("")
    lines.append(f"🗺️ Map: {map_url}")
    lines.append("")
    lines.append("Reply STOP to unsubscribe.")

    if mpd_warning:
        lines.append("")
        lines.append("⚠️ MPD data may be delayed — we'll catch you up tomorrow.")

    return "\n".join(lines)
