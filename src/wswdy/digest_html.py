"""HTML email renderer for the daily digest.

The WhatsApp text (wswdy.digest) stays the canonical plain-text body; this
module renders the same data as a structured HTML email: real headings,
severity chips in the site's tier palette, the static map image up top,
and the map link as a button. The email notifier drops this into its
outer shell (background, unsubscribe footer).

All styles are inline — email clients strip <style> blocks.
"""
from wswdy.address import humanize_address
from wswdy.digest import (
    _arrest_full_name,
    _fmt_blocks,
    _fmt_time,
    _select_closest_arrests,
    _summarize_arrests_by_tier,
    _summarize_crashes_by_tier,
    _summary_line,
    _tier_examples,
    select_closest,
    summarize_by_tier,
)
from wswdy.offenses import humanize_offense
from wswdy.tiers import classify_arrest

# Site tier palette (shared.css --t1..--t4) + black for fatal, grey for
# unspecified.
_CRIME_TIER_COLOR = {1: "#DC2626", 2: "#EA580C", 3: "#D97706", 4: "#65A30D"}
_CRIME_TIER_LABEL = {1: "violent", 2: "serious property", 3: "vehicle", 4: "petty"}
_CRASH_TIER_COLOR = {1: "#0A0A0A", 2: "#DC2626", 3: "#D97706", 4: "#65A30D"}
_CRASH_TIER_LABEL = {
    1: "fatal", 2: "major injuries", 3: "minor injuries", 4: "property damage",
}
_ARREST_TIER_COLOR = {1: "#DC2626", 2: "#D97706", 3: "#A3A3A3"}
_ARREST_TIER_LABEL_SINGULAR = {1: "felony", 2: "misdemeanor", 3: "unspecified"}
_ARREST_TIER_LABEL_PLURAL = {1: "felonies", 2: "misdemeanors", 3: "unspecified"}

_FONT = "-apple-system, system-ui, 'Segoe UI', sans-serif"


def _esc(s: str) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _dot(color: str) -> str:
    """A small severity dot — the email equivalent of the emoji glyphs."""
    return (
        f"<span style='display:inline-block;width:10px;height:10px;"
        f"border-radius:50%;background:{color};margin-right:7px;"
        f"vertical-align:middle;'></span>"
    )


def _h2(text: str, sub: str = "") -> str:
    sub_html = (
        f" <span style='font-weight:400;color:#737373;font-size:13px;'>{_esc(sub)}</span>"
        if sub else ""
    )
    return (
        f"<h2 style='font:600 15px/1.3 {_FONT};margin:30px 0 12px;"
        f"color:#0A0A0A;'>{_esc(text)}{sub_html}</h2>"
    )


def _row(dot_color: str, text_html: str) -> str:
    return (
        f"<div style='font:14px/1.6 {_FONT};color:#404040;margin:6px 0;'>"
        f"{_dot(dot_color)}{text_html}</div>"
    )


def _muted(text: str) -> str:
    return (
        f"<div style='font:14px/1.6 {_FONT};color:#737373;margin:6px 0;'>{_esc(text)}</div>"
    )


def build_digest_html(
    *,
    display_name: str,
    radius_m: int,
    crimes: list[dict],
    home_lat: float,
    home_lon: float,
    map_url: str,
    crashes: list[dict] | None = None,
    new_crash_count: int = 0,
    arrests: list[dict] | None = None,
    have_arrest_today: bool = False,
    mpd_warning: bool = False,
    date_label: str = "",
    has_image: bool = False,
) -> str:
    """Render the digest as inner-body HTML (no <html>/<body> shell —
    the email notifier wraps it). ``has_image`` places the cid:preview
    static-map image right under the header."""
    radius_str = f"{radius_m:,}m"
    parts: list[str] = []

    # Header + TL;DR
    title = f"WTFDC for {display_name}"
    if date_label:
        title += f" — {date_label}"
    parts.append(
        f"<h1 style='font:700 19px/1.3 {_FONT};margin:0 0 8px;color:#0A0A0A;'>"
        f"{_esc(title)} ☀️</h1>"
    )
    summary = _summary_line(
        crimes=crimes, crashes=crashes, new_crash_count=new_crash_count,
        arrests=arrests, have_arrest_today=have_arrest_today,
    )
    parts.append(
        f"<div style='font:15px/1.6 {_FONT};color:#404040;margin-bottom:12px;'>"
        f"{_esc(summary)}</div>"
    )

    # Static map right up top — the most glanceable artifact we produce.
    if has_image:
        parts.append(
            "<img src='cid:preview' alt='Map of incidents near you' "
            "style='display:block;width:100%;max-width:560px;margin:12px 0;"
            "border:1px solid #E5E3DC;border-radius:10px;' />"
        )

    # Crimes
    n = len(crimes)
    if n == 0:
        parts.append(_h2("Crimes", "last 24h"))
        parts.append(_muted(
            f"Quiet night — 0 crimes reported within {radius_str} of your home."
        ))
    else:
        parts.append(_h2("Crimes", f"last 24h · your {radius_str} zone"))
        counts = summarize_by_tier(crimes)
        for tier in (1, 2, 3, 4):
            c = counts[tier]
            if c == 0:
                continue
            examples = _tier_examples(crimes, tier)
            label = f"<strong>{c} {_esc(_CRIME_TIER_LABEL[tier])}</strong>"
            if examples:
                label += f" — {_esc(examples)}"
            parts.append(_row(_CRIME_TIER_COLOR[tier], label))

        closest = select_closest(
            crimes, home_lat=home_lat, home_lon=home_lon,
            radius_m=radius_m, max_items=3,
        )
        if closest:
            parts.append(
                f"<div style='font:600 13px/1.6 {_FONT};color:#737373;"
                f"margin:16px 0 6px;'>NEAREST TO YOU</div>"
            )
            for c in closest:
                offense = humanize_offense(c["offense"], c.get("method"))
                t = _fmt_time(c["report_dt"])
                parts.append(
                    f"<div style='font:14px/1.6 {_FONT};color:#404040;margin:6px 0;'>"
                    f"• {_esc(offense)} — {_esc(_fmt_blocks(c['distance_m']))} "
                    f"<span style='color:#737373;'>"
                    f"({_esc(humanize_address(c['block_address']))}, {_esc(t)})"
                    f"</span></div>"
                )

    # Crashes
    if crashes is not None:
        parts.append(_h2("Crashes", "last 7 days"))
        if not crashes:
            parts.append(_muted("No crashes near you in the last 7 days."))
        else:
            if new_crash_count > 0:
                parts.append(
                    f"<div style='font:14px/1.6 {_FONT};color:#0A0A0A;margin:6px 0;'>"
                    f"<strong>{new_crash_count} newly reported this week.</strong></div>"
                )
            ccounts = _summarize_crashes_by_tier(crashes)
            for tier in (1, 2, 3, 4):
                c = ccounts[tier]
                if c == 0:
                    continue
                parts.append(_row(
                    _CRASH_TIER_COLOR[tier],
                    f"{c} {_esc(_CRASH_TIER_LABEL[tier])}",
                ))
            for c in crashes:
                if (c.get("fatal") or 0) > 0:
                    parts.append(_row(
                        "#0A0A0A",
                        f"<strong>Fatal crash</strong> — "
                        f"{_esc(c.get('address') or 'address unknown')}",
                    ))
                elif (c.get("ped_major") or 0) > 0:
                    parts.append(_row(
                        "#DC2626",
                        f"<strong>Pedestrian struck</strong> — "
                        f"{_esc(c.get('address') or 'address unknown')}",
                    ))
                elif (c.get("bike_major") or 0) > 0:
                    parts.append(_row(
                        "#DC2626",
                        f"<strong>Cyclist struck</strong> — "
                        f"{_esc(c.get('address') or 'address unknown')}",
                    ))

    # Arrests
    if arrests is not None:
        parts.append(_h2("Arrests", "last 24h"))
        if not have_arrest_today:
            parts.append(_muted(
                "No arrest report yet today — usually arrives mid-morning."
            ))
        elif not arrests:
            parts.append(_muted("No arrests near you in the last 24h."))
        else:
            acounts = _summarize_arrests_by_tier(arrests)
            for tier in (1, 2, 3):
                c = acounts[tier]
                if c == 0:
                    continue
                label = (_ARREST_TIER_LABEL_PLURAL if c != 1
                         else _ARREST_TIER_LABEL_SINGULAR)[tier]
                parts.append(_row(_ARREST_TIER_COLOR[tier], f"{c} {_esc(label)}"))
            closest_a = _select_closest_arrests(
                arrests, home_lat=home_lat, home_lon=home_lon,
                radius_m=radius_m, max_items=2,
            )
            if closest_a:
                parts.append(
                    f"<div style='font:600 13px/1.6 {_FONT};color:#737373;"
                    f"margin:16px 0 6px;'>NEAREST</div>"
                )
                for a in closest_a:
                    tier = classify_arrest(a)
                    tl = _ARREST_TIER_LABEL_SINGULAR[tier].capitalize()
                    offense = (a.get("offense") or "Unknown offense").strip()
                    name = _arrest_full_name(a)
                    bits = []
                    if a.get("age"):
                        bits.append(str(a["age"]))
                    g = (a.get("gender") or "").strip()
                    if g:
                        bits.append(g[0].upper())
                    demo = f" ({name}, {', '.join(bits)})" if name and bits \
                        else f" ({name})" if name else ""
                    addr = humanize_address(a.get("arrest_location") or "")
                    when = _fmt_time(a["arrest_dt"]) if a.get("arrest_dt") else ""
                    meta_bits = [addr]
                    if "distance_m" in a:
                        meta_bits.append(_fmt_blocks(a["distance_m"]))
                    if when:
                        meta_bits.append(when)
                    parts.append(
                        f"<div style='font:14px/1.6 {_FONT};color:#404040;margin:8px 0;'>"
                        f"• <strong>{_esc(tl)}</strong> — {_esc(offense)}{_esc(demo)}"
                        f"<br /><span style='color:#737373;padding-left:14px;'>"
                        f"📍 {_esc(' · '.join(meta_bits))}</span></div>"
                    )

    # Map button
    parts.append(
        f"<div style='margin:30px 0 6px;'>"
        f"<a href='{_esc(map_url)}' style='display:inline-block;"
        f"background:#0A0A0A;color:#FFFFFF;text-decoration:none;"
        f"font:600 14px/1 {_FONT};padding:12px 22px;border-radius:10px;'>"
        f"Open your map</a></div>"
    )

    if mpd_warning:
        parts.append(
            f"<div style='font:13px/1.6 {_FONT};color:#92400E;background:#FEF3C7;"
            f"border-radius:8px;padding:10px 14px;margin-top:14px;'>"
            f"⚠️ MPD data may be delayed — we'll catch you up tomorrow.</div>"
        )

    return "\n".join(parts)
