"""Address humanization for display.

MPD and the DC crashes feed publish addresses in shouty UPPERCASE
("1721 - 1799 BLOCK OF 19TH STREET NW", "1500 14TH ST NW"). Convert to
title case with US street conventions:

  - directionals (NW, NE, SW, SE, N, S, E, W) stay UPPERCASE
  - ordinal street numbers ("19TH") get lowercased suffix → "19th"
  - small connecting words ("of", "and") stay lowercase mid-string
  - " - " between block ranges becomes an en-dash ("–")

Pure presentation — internal storage and any spatial/text matching
should still use the raw value.
"""
import re

# US street directionals — always uppercase no matter where they appear.
_DIRECTIONALS = frozenset({"N", "S", "E", "W", "NE", "NW", "SE", "SW"})

# Small words that stay lowercase mid-title (book-title convention).
_LOWER_WORDS = frozenset({
    "of", "and", "the", "a", "an", "in", "on", "at", "to",
    "with", "from", "by", "for", "or", "nor", "but", "as",
})

# Detect range hyphens like "1721 - 1799" so we can swap to en-dash.
_RANGE_HYPHEN = re.compile(r"\s+-\s+")


def humanize_address(addr: str | None) -> str:
    """Return a friendly Title Case rendering of a US street address."""
    if not addr:
        return ""
    s = _RANGE_HYPHEN.sub("–", addr.strip())  # en-dash for ranges
    tokens = s.split()
    out: list[str] = []
    for i, tok in enumerate(tokens):
        if tok.upper() in _DIRECTIONALS:
            out.append(tok.upper())
            continue
        if i > 0 and tok.lower() in _LOWER_WORDS:
            out.append(tok.lower())
            continue
        # str.capitalize() lowercases everything then upper-cases the first
        # char — happens to handle numeric ordinals correctly:
        #   "19TH".capitalize() == "19th", "1ST".capitalize() == "1st".
        out.append(tok.capitalize())
    return " ".join(out)
