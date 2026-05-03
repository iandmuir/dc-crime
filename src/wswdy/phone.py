"""Phone number normalization for WhatsApp delivery.

The WhatsApp bridge requires E.164 format (``+<country><number>``). If the
signup form receives ``9174945082`` (US 10-digit) the bridge silently
accepts the message but it never delivers — there's no country code so
WhatsApp can't route it. Normalize early; reject what we can't fix.

Rules
-----
- Strip everything that isn't a digit or a plus.
- If it already starts with ``+``, validate length (8-15 digits) and return.
- 11 digits starting with ``1`` → prepend ``+`` (US/Canada with country code).
- Exactly 10 digits → assume US, prepend ``+1``. This is a small
  forgiving-default: most subscribers are local, the form's placeholder
  shows a US example, and the alternative (silent delivery failure) is
  worse than a one-country assumption that's ~always right for this
  service.
- Anything else raises ``InvalidPhoneNumber`` so the form returns 400
  with a clear message instead of accepting a number we know won't work.
"""
from __future__ import annotations

import re

_DIGITS_RE = re.compile(r"[^\d+]")


class InvalidPhoneNumber(ValueError):
    """Raised when an input string can't be coerced into E.164."""


def normalize_phone(raw: str | None) -> str:
    """Return an E.164-formatted phone number (e.g. ``+12024681234``).

    Raises ``InvalidPhoneNumber`` if the input is empty or can't be
    confidently coerced.
    """
    if not raw:
        raise InvalidPhoneNumber("phone number is empty")
    s = _DIGITS_RE.sub("", raw.strip())
    if not s:
        raise InvalidPhoneNumber("no digits in phone number")

    if s.startswith("+"):
        digits = s[1:]
        if not digits.isdigit():
            raise InvalidPhoneNumber("phone number contains non-digits after '+'")
        if not (8 <= len(digits) <= 15):
            raise InvalidPhoneNumber(
                f"phone number length {len(digits)} outside E.164 range (8-15)"
            )
        return "+" + digits

    # No '+'. Apply the US-default rules.
    if len(s) == 10 and s.isdigit():
        return "+1" + s
    if len(s) == 11 and s.startswith("1") and s.isdigit():
        return "+" + s
    raise InvalidPhoneNumber(
        "phone number must be in E.164 format (e.g. +1 202 555 0123) "
        "or a 10-digit US number"
    )
