"""Inline SVG icon strings used by the map page.

Kept tiny and color-agnostic (currentColor) so the surrounding tier-tinted
container drives the look. Both icons are 22×22 — that's the size the map
markers render at and the legend swatches reuse the same markup. A single
source of truth for the silhouettes means the legend, layer toggle, and
the actual map markers can never disagree.

Sources: hand-drawn against a 24px viewBox, no external dependencies.
"""

# Top-down car silhouette. Used for crash markers + the crash legend swatch.
ICON_CAR_SVG = (
    '<svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor" '
    'aria-hidden="true">'
    '<path d="M5 11l1.5-4.5C6.8 5.6 7.6 5 8.5 5h7c.9 0 1.7.6 2 1.5L19 11'
    'h.5c.8 0 1.5.7 1.5 1.5V17a1 1 0 0 1-1 1h-1.5v.5a1.5 1.5 0 0 1-3 0V18'
    'h-7v.5a1.5 1.5 0 0 1-3 0V18H4a1 1 0 0 1-1-1v-4.5c0-.8.7-1.5 1.5-1.5H5z'
    'M7 14a1 1 0 1 0 0-2 1 1 0 0 0 0 2zm10 0a1 1 0 1 0 0-2 1 1 0 0 0 0 2z'
    'M6.8 11h10.4l-1-3a1 1 0 0 0-1-.7H8.8a1 1 0 0 0-1 .7l-1 3z"/>'
    '</svg>'
)

# Sheriff/police 5-point star badge. Used for arrest markers + the arrest
# legend swatch.
ICON_BADGE_SVG = (
    '<svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor" '
    'aria-hidden="true">'
    '<path d="M12 2.5l2.39 5.55 6.04.46-4.6 3.94 1.45 5.88L12 15.18'
    'l-5.28 3.15 1.45-5.88-4.6-3.94 6.04-.46L12 2.5z"/>'
    '<circle cx="12" cy="11.5" r="1.6" fill="white" fill-opacity="0.85"/>'
    '</svg>'
)
