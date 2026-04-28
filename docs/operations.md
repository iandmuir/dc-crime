# wswdy — Operations Runbook

## Daily checks (passive)
- Watch for the **23:00 ET health email**. If it doesn't arrive, log in to the LXC.
- Watch for any `[wswdy] <alert_type>` admin alert email or HA push.

## On admin alert: `mpd_down`
The MPD feed has been unreachable for >1 hour.
1. `curl -sS "https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/FeatureServer/39/query?outFields=*&where=1%3D1&resultRecordCount=1&f=geojson" | head`
2. If MPD is down externally → wait. The next morning's digests will show the "MPD data may be delayed" warning. Suppression auto-clears after 6h.
3. If MPD is back but our cron didn't catch up → SSH in and run a manual fetch:
   ```bash
   sudo -u wswdy /opt/wswdy/.venv/bin/python -c \
     "import asyncio; from wswdy.config import get_settings; \
      from wswdy.db import connect, init_schema; \
      from wswdy.alerts import AdminAlerter; \
      from wswdy.notifiers.email import EmailNotifier; \
      from wswdy.jobs.fetch import run_fetch; \
      s = get_settings(); db = connect(s.db_path); init_schema(db); \
      e = EmailNotifier(host=s.smtp_host, port=s.smtp_port, user=s.smtp_user, password=s.smtp_pass, sender=s.smtp_from); \
      a = AdminAlerter(db=db, email=e, admin_email=s.admin_email, ha_webhook_url=s.ha_webhook_url); \
      asyncio.run(run_fetch(db=db, feed_url=str(s.mpd_feed_url), alerter=a))"
   ```

## On admin alert: `whatsapp_session_expired`
The MCP needs a fresh QR scan.
1. Open the device that holds the +12024682709 WhatsApp Business session.
2. Settings → Linked Devices → Link a Device → scan the QR shown by the MCP UI in its LXC.
3. Verify with a manual welcome message via `/admin` console (or by approving a pending sub).

## On admin alert: SMTP failure
The job retried for 6h. Check the SMTP provider's status page; rotate creds if needed; restart the service:
```bash
systemctl restart dccrime
```

## Service restart
```bash
systemctl restart dccrime
journalctl -u dccrime -n 50
```

## Tunnel down
```bash
systemctl status cloudflared
journalctl -u cloudflared -n 50
# If the tunnel is down, the daily SEND still runs (no inbound HTTP needed).
# Map links and signup are unreachable until the tunnel returns.
```

## Restoring from backup
```bash
systemctl stop dccrime
cd /tmp && rclone copy gdrive:wswdy-backups/wswdy-<TS>.tar.gz .
tar -xzf wswdy-<TS>.tar.gz
sudo -u wswdy cp dccrime.db /opt/wswdy/dccrime.db
sudo -u wswdy cp .env       /opt/wswdy/.env
systemctl start dccrime
```

## Removing a subscriber manually
```bash
sqlite3 /opt/wswdy/dccrime.db \
  "UPDATE subscribers SET status='UNSUBSCRIBED', unsubscribed_at=CURRENT_TIMESTAMP WHERE id='<sid>';"
```

## Migrating to WhatsApp Cloud API later
Implement `WhatsAppCloudNotifier(Notifier)` in `src/wswdy/notifiers/whatsapp_cloud.py`,
swap the binding in `main.py:create_app()`. The Notifier protocol is the only seam.
