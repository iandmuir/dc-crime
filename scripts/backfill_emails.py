#!/usr/bin/env python3
"""Re-ingest every email currently in Resend's received-mail inbox.

Used to backfill data after a parser fix — e.g. when we missed a date
format and a chunk of arrests landed with NULL ``arrest_dt``. Walks
Resend's list-received-emails API page by page, fetches each email's
full body, and runs the same email-ingest job the live webhook uses.
The ingest is upsert-by-key, so re-processing already-ingested rows
just refreshes them; no duplicates created.

Usage::

    python scripts/backfill_emails.py                   # all emails
    python scripts/backfill_emails.py --limit 50        # cap pagination
    python scripts/backfill_emails.py --subject crime   # only matching subjects

Run on the server (where settings + the SQLite DB live)::

    /root/wswdy/.venv/bin/python scripts/backfill_emails.py
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

# Make the source tree importable when run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wswdy.clients.resend_inbound import (  # noqa: E402
    get_received_email,
    list_received_emails,
)
from wswdy.config import get_settings  # noqa: E402
from wswdy.db import connect, init_schema  # noqa: E402
from wswdy.jobs.email_ingest import ingest_email  # noqa: E402


async def _run(*, max_emails: int | None, subject_filter: str | None) -> int:
    settings = get_settings()
    if not settings.resend_api_key:
        print("ERROR: RESEND_API_KEY not configured", file=sys.stderr)
        return 1

    db = connect(settings.db_path)
    init_schema(db)

    after: str | None = None
    seen = ingested = skipped = errored = 0

    while True:
        page = await list_received_emails(
            api_key=settings.resend_api_key, limit=100, after=after,
        )
        items = page.get("data") or []
        if not items:
            break

        for item in items:
            seen += 1
            email_id = item["id"]
            subject = item.get("subject") or ""
            if subject_filter and subject_filter.lower() not in subject.lower():
                skipped += 1
                continue
            try:
                # The list endpoint doesn't include the body; fetch the full
                # email so the parser has the plain-text version.
                full = await get_received_email(
                    email_id, api_key=settings.resend_api_key,
                )
                text_body = full.get("text") or ""
                if not text_body:
                    print(f"  [skip no-body] {email_id} {subject!r}")
                    skipped += 1
                    continue
                res = await ingest_email(
                    db=db, subject=subject, text_body=text_body,
                    source_file=email_id,
                    maptiler_api_key=settings.maptiler_api_key,
                )
                ingested += 1
                print(f"  [{res.get('status')}] {email_id} ({res.get('kind')}, "
                      f"{res.get('district')}): +{res.get('added', 0)} / "
                      f"~{res.get('updated', 0)}")
            except Exception as e:  # noqa: BLE001
                errored += 1
                print(f"  [error] {email_id} {subject!r}: {e}",
                      file=sys.stderr)

            if max_emails is not None and seen >= max_emails:
                break

        if max_emails is not None and seen >= max_emails:
            break
        if not page.get("has_more"):
            break
        after = items[-1]["id"]

    print()
    print(f"--- seen: {seen}, ingested: {ingested}, "
          f"skipped: {skipped}, errors: {errored} ---")
    return 0 if errored == 0 else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--limit", type=int, default=None,
        help="Stop after N total emails (default: process everything)",
    )
    ap.add_argument(
        "--subject", default=None,
        help="Only re-ingest emails whose subject contains this string "
             "(case-insensitive). Useful to backfill just arrests, e.g. "
             "--subject 'arrest report'",
    )
    args = ap.parse_args()
    return asyncio.run(_run(
        max_emails=args.limit, subject_filter=args.subject,
    ))


if __name__ == "__main__":
    raise SystemExit(main())
