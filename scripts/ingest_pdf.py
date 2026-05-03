#!/usr/bin/env python3
"""Manually parse one or more MPD daily report PDFs and pretty-print the
extracted records. Used during Phase 1 development to validate the parser
against real PDFs before the email-ingestion plumbing is wired up.

Usage:
    python scripts/ingest_pdf.py /path/to/file.pdf [/path/to/another.pdf ...]
    python scripts/ingest_pdf.py /path/to/dir/

When given a directory, processes all *.pdf files inside it. Each PDF is
auto-routed to the crime or arrest parser based on the report's header
text.
"""
from __future__ import annotations

import argparse
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("paths", nargs="+", help="PDF files or directories")
    ap.add_argument("--json", action="store_true",
                    help="Emit JSON Lines output instead of human-readable")
    ap.add_argument("--quiet", action="store_true",
                    help="Don't print per-file headers")
    args = ap.parse_args()

    pdfs = _gather_pdfs(args.paths)
    if not pdfs:
        print("no PDFs found", file=sys.stderr)
        return 1

    total_crimes = 0
    total_arrests = 0
    for pdf in pdfs:
        kind = detect_pdf_kind(pdf)
        if not args.quiet:
            print(f"\n=== {pdf.name}  ({kind or 'unknown'}) ===")
        if kind == "crime":
            records = parse_crime_pdf(pdf)
            total_crimes += len(records)
            for r in records:
                d = asdict(r)
                if args.json:
                    print(json.dumps({"kind": "crime", **d}))
                else:
                    print(f"  CCN {d['ccn']}: {d.get('offense')} "
                          f"@ {d.get('block')!r} ({d.get('location')})")
        elif kind == "arrest":
            records = parse_arrest_pdf(pdf)
            total_arrests += len(records)
            for r in records:
                d = asdict(r)
                if args.json:
                    print(json.dumps({"kind": "arrest", **d}))
                else:
                    name = " ".join(filter(None, [
                        d.get("offender_first_name"), d.get("offender_last_name"),
                    ])) or "?"
                    print(f"  Arrest {d['arrest_number']}: {name} "
                          f"({d.get('age')}) — {d.get('offense')!r}")
        else:
            print(f"  ! could not detect report kind, skipping")

    if not args.quiet:
        print(f"\n--- total: {total_crimes} crimes, {total_arrests} arrests ---")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
