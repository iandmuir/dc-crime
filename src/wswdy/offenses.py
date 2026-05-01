"""Humanize MPD offense codes and weapon methods for display.

MPD publishes offenses as clinical UPPERCASE strings ("THEFT F/AUTO",
"ASSAULT W/DANGEROUS WEAPON"). These are stable identifiers that classify()
in wswdy.tiers can match against, but they read as shouty cop-speak in
the digest and map popups. Map them to friendly Title Case labels at
display time only — internal storage and tier classification still use
the raw codes.
"""

# Keyed on the exact uppercase code MPD publishes. New codes get a
# title-cased fallback in humanize_offense; add them here when we
# notice them and want a nicer label.
OFFENSE_LABELS: dict[str, str] = {
    "HOMICIDE": "Homicide",
    "SEX ABUSE": "Sex Abuse",
    "ASSAULT W/DANGEROUS WEAPON": "Assault with Dangerous Weapon",
    "ROBBERY": "Robbery",
    "BURGLARY": "Burglary",
    "ARSON": "Arson",
    "MOTOR VEHICLE THEFT": "Motor Vehicle Theft",
    "THEFT/OTHER": "Theft (Other)",
    "THEFT F/AUTO": "Theft from Auto",
}

# Method humanization. Only weapon-of-note values map to a label; "OTHERS"
# (MPD's catch-all residual) intentionally returns None so the popup
# doesn't show an uninformative "weapon: other" row.
_METHOD_LABELS: dict[str, str] = {
    "GUN": "Gun",
    "KNIFE": "Knife",
}

_ARMED_METHODS = {"GUN", "KNIFE"}


def humanize_offense(offense: str | None, method: str | None = None) -> str:
    """Return a friendly display label for an MPD offense.

    Special case: armed robbery (robbery + gun/knife) collapses to
    "Armed robbery" rather than plain "Robbery" — different risk profile
    for a neighbor reading the digest, worth distinguishing in the
    headline rather than burying in a separate weapon row.
    """
    if not offense:
        return "Crime"
    o = offense.strip().upper()
    m = (method or "").strip().upper()
    if o == "ROBBERY" and m in _ARMED_METHODS:
        return "Armed Robbery"
    if o in OFFENSE_LABELS:
        return OFFENSE_LABELS[o]
    # Unknown offense — best-effort title case so we don't shout at the user.
    return offense.strip().title()


def humanize_method(method: str | None) -> str | None:
    """Return a friendly weapon label, or None for absent / non-noteworthy.

    None signals "don't render a weapon row in the popup" — keeps display
    code branch-free."""
    if not method:
        return None
    return _METHOD_LABELS.get(method.strip().upper())
