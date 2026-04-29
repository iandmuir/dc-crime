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


# ----- Crash tier classification ----------------------------------------
# Independent severity hierarchy (DC crashes have different fields than
# crimes — injury counts per-role, fatality flags, etc.) but we reuse the
# 1..4 tier convention so the digest can render them with the same glyphs.
#
# Tier 1: any fatality
# Tier 2: any major injury (extra weight if pedestrian or cyclist)
# Tier 3: any minor injury
# Tier 4: property damage only
_CRASH_LABELS: Final = {
    1: "fatal", 2: "major injuries", 3: "minor injuries", 4: "property damage",
}


def classify_crash(crash: dict) -> int:
    """Return tier 1..4 for a crash dict (matches the crashes-table column names)."""
    if (crash.get("fatal") or 0) > 0:
        return 1
    if (crash.get("major_injury") or 0) > 0:
        return 2
    if (crash.get("minor_injury") or 0) > 0:
        return 3
    return 4


def crash_tier_label(tier: int) -> str:
    return _CRASH_LABELS[tier]
