#!/usr/bin/env bash
# Daily SQLite + .env backup. Designed to be invoked from cron at ~02:00 ET.
# Usage: backup.sh <rclone-remote-name>:<path>
# E.g.:  backup.sh gdrive:wswdy-backups
set -euo pipefail

DEST="${1:?usage: backup.sh <rclone-remote:path>}"
APP_DIR="${WSWDY_APP_DIR:-/opt/wswdy}"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cp "$APP_DIR/dccrime.db" "$WORK/dccrime.db"
sqlite3 "$WORK/dccrime.db" "PRAGMA wal_checkpoint(TRUNCATE);" >/dev/null
cp "$APP_DIR/.env" "$WORK/.env"
tar -C "$WORK" -czf "$WORK/wswdy-$TS.tar.gz" dccrime.db .env
rclone copy "$WORK/wswdy-$TS.tar.gz" "$DEST/" --quiet

# Retention: keep last 14
rclone lsf "$DEST/" --include "wswdy-*.tar.gz" | sort | head -n -14 | while read -r f; do
  rclone delete "$DEST/$f" --quiet || true
done
