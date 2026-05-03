#!/usr/bin/env python3
"""Manually parse one or more MPD daily report PDFs.

Two modes:

  - Default (preview): pretty-print the parsed records. Used during
    development to validate the parser against real PDFs without
    touching the database.

  - ``--persist``: parse + upsert into the configured SQLite DB and
    record a coverage row in pdf_ingest_log. Arrest PDFs also trigger
    MapTiler geocoding for any new rows. Idempotent — re-running over
    the same PDF refreshes existing rows but doesn't double-insert.

Usage::

    python scripts/ingest_pdf.py /path/to/file.pdf [/path/to/another.pdf ...]
    python scripts/ingest_pdf.py /path/to/dir/
    python scripts/ingest_pdf.py --persist /path/to/dir/

When given a directory, processes all *.pdf files inside it. Each PDF is
auto-routed to the crime or arrest parser based on the report's header
text.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from pathlib import Path

# When invoked directly (not via `python -m wswdy.scripts...`), make sure
# the package is importable from the source tree.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wswdy.pdf_reports import (  # noqa: E402
    detect_pdf_kind,
    parse_arrest_pdf,
    parse_crime_pdf,
)


def _gather_pdfs(paths: list[str]) -> list[Path]:
    out: list[Path] = []
    for p in paths:
        path = Path(p)
        if path.is_dir():
            out.extend(sorted(path.glob("*.pdf")))
        elif path.is_file():
            out.append(path)
        else:
            print(f"warning: skipping {p} (not a file or directory)",
                  file=sys.stderr)
    return out


def _preview(pdfs: list[Path], *, as_json: bool, quiet: bool) -> int:
    total_crimes = 0
    total_arrests = 0
    for pdf in pdfs:
        kind = detect_pdf_kind(pdf)
        if not quiet:
            print(f"\n=== {pdf.name}  ({kind or 'unknown'}) ===")
        if kind == "crime":
            records = parse_crime_pdf(pdf)
            total_crimes += len(records)
            for r in records:
                d = asdict(r)
                if as_json:
                    print(json.dumps({"kind": "crime", **d}))
                else:
                    print(f"  CCN {d['ccn']}: {d.get('offense')} "
                          f"@ {d.get('block')!r} ({d.get('location')})")
        elif kind == "arrest":
            records = parse_arrest_pdf(pdf)
            total_arrests += len(records)
            for r in records:
                d = asdict(r)
                if as_json:
                    print(json.dumps({"kind": "arrest", **d}))
                else:
                    name = " ".join(filter(None, [
                        d.get("offender_first_name"), d.get("offender_last_name"),
                    ])) or "?"
                    print(f"  Arrest {d['arrest_number']}: {name} "
                          f"({d.get('age')}) — {d.get('offense')!r}")
        else:
            print("  ! could not detect report kind, skipping")

    if not quiet:
        print(f"\n--- total: {total_crimes} crimes, {total_arrests} arrests ---")
    return 0


async def _persist(pdfs: list[Path], *, quiet: bool) -> int:
    # Imports deferred so the preview path doesn't require DB / settings.
    from wswdy.config import get_settings  # noqa: E402
    from wswdy.db import connect, init_schema  # noqa: E402
    from wswdy.jobs.pdf_ingest import ingest_pdf  # noqa: E402

    settings = get_settings()
    db = connect(settings.db_path)
    init_schema(db)

    rc = 0
    for pdf in pdfs:
        if not quiet:
            print(f"\n=== {pdf.name} ===")
        try:
            res = await ingest_pdf(
                db=db, path=pdf,
                maptiler_api_key=settings.maptiler_api_key,
            )
        except Exception as e:  # noqa: BLE001
            print(f"  ! ingest failed: {e}", file=sys.stderr)
            rc = 1
            continue
        if not quiet:
            print(f"  {res}")
    return rc


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("paths", nargs="+", help="PDF files or directories")
    ap.add_argument("--persist", action="store_true",
                    help="Upsert into the database (otherwise: preview only)")
    ap.add_argument("--json", action="store_true",
                    help="Emit JSON Lines output instead of human-readable "
                         "(preview mode only)")
    ap.add_argument("--quiet", action="store_true",
                    help="Don't print per-file headers")
    args = ap.parse_args()

    pdfs = _gather_pdfs(args.paths)
    if not pdfs:
        print("no PDFs found", file=sys.stderr)
        return 1

    if args.persist:
        return asyncio.run(_persist(pdfs, quiet=args.quiet))
    return _preview(pdfs, as_json=args.json, quiet=args.quiet)


if __name__ == "__main__":
    raise SystemExit(main())
