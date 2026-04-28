"""Short URL-safe random IDs for subscribers."""
import secrets


def new_subscriber_id() -> str:
    """Return ~43 bits of entropy as an 8-char URL-safe string."""
    return secrets.token_urlsafe(6)
