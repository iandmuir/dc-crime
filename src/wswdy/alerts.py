"""Admin alerter — emails the admin + posts a Home Assistant webhook,
suppressing repeats of the same alert_type for a configurable window."""
import logging
import sqlite3
from datetime import UTC, datetime, timedelta

import httpx

from wswdy.notifiers.base import Notifier
from wswdy.repos.admin_alerts import is_suppressed, record, set_suppressed_until

log = logging.getLogger(__name__)


class AdminAlerter:
    """Sends admin alerts via email + optional HA webhook, with per-type suppression."""

    def __init__(
        self,
        *,
        db: sqlite3.Connection,
        email: Notifier,
        admin_email: str,
        ha_webhook_url: str,
        suppression_hours: int = 6,
    ):
        self.db = db
        self.email = email
        self.admin_email = admin_email
        self.ha_webhook_url = ha_webhook_url
        self.suppression_hours = suppression_hours

    async def alert(self, *, alert_type: str, message: str) -> None:
        """Send an admin alert, unless suppressed.

        On first fire: records the alert, suppresses for `suppression_hours`,
        emails the admin, and fires the HA webhook (if configured).
        """
        if is_suppressed(self.db, alert_type):
            log.debug("Alert %s suppressed, skipping", alert_type)
            return

        record(self.db, alert_type=alert_type, message=message)
        until = (
            datetime.now(UTC) + timedelta(hours=self.suppression_hours)
        ).isoformat(timespec="seconds")
        set_suppressed_until(self.db, alert_type, until)

        subject = f"[wswdy] {alert_type}"
        text = f"{message}\n\n— wswdy admin alerter\n(suppressed for {self.suppression_hours}h)"
        await self.email.send(
            recipient=self.admin_email, subject=subject, text=text, image_path=None
        )

        if self.ha_webhook_url:
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    await client.post(
                        self.ha_webhook_url,
                        json={"alert_type": alert_type, "message": message},
                    )
            except Exception:
                # HA being down is not itself alert-worthy; email is already sent.
                log.warning("HA webhook failed for alert_type=%s", alert_type)
