"""Notifier protocol and supporting types."""
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class SendResult:
    ok: bool
    error: str | None = None
    detail: str | None = None  # provider-specific detail (message id, etc.)


@runtime_checkable
class Notifier(Protocol):
    async def send(self, *, recipient: str, subject: str, text: str,
                   image_path: Path | None) -> SendResult: ...
