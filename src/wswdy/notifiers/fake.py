"""In-memory notifier for tests."""
from pathlib import Path

from wswdy.notifiers.base import Notifier, SendResult


class FakeNotifier(Notifier):
    def __init__(self, fail_with: str | None = None):
        self.sent: list[dict] = []
        self.fail_with = fail_with

    async def send(self, *, recipient: str, subject: str, text: str,
                   image_path: Path | None) -> SendResult:
        self.sent.append({"recipient": recipient, "subject": subject,
                          "text": text, "image_path": image_path})
        if self.fail_with:
            return SendResult(ok=False, error=self.fail_with)
        return SendResult(ok=True)
