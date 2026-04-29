# Deploying wswdy to the NUC

## Prerequisites
- Proxmox LXC: Debian 12 minimal, 1 vCPU, 512 MB RAM, 4 GB disk
- Domain `iandmuir.com` on Cloudflare
- Existing WhatsApp MCP LXC reachable from this LXC

## 1. Create the LXC
```bash
# On the Proxmox host:
pct create <ID> local:vztmpl/debian-12-standard.tar.zst \
  --hostname dc-crime-app --memory 512 --cores 1 --rootfs local-lvm:4 \
  --net0 name=eth0,bridge=vmbr0,ip=dhcp \
  --features nesting=1 --unprivileged 1
pct start <ID>
pct enter <ID>
```

## 2. System setup
```bash
apt-get update && apt-get install -y python3.12 python3.12-venv git curl
adduser --system --group --home /opt/wswdy wswdy
mkdir -p /var/log/dccrime
chown wswdy:wswdy /var/log/dccrime
```

## 3. Clone + install
```bash
sudo -u wswdy bash -c "
  cd /opt/wswdy &&
  git clone https://github.com/iandmuir/dc-crime.git . &&
  python3.12 -m venv .venv &&
  .venv/bin/pip install -r requirements.txt &&
  .venv/bin/pip install -e .
"
```

## 4. Configure secrets
```bash
sudo -u wswdy cp /opt/wswdy/.env.example /opt/wswdy/.env
chmod 600 /opt/wswdy/.env
$EDITOR /opt/wswdy/.env  # fill in MAPTILER_API_KEY, SMTP, MCP, HMAC_SECRET, etc.

# Generate strong secrets:
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'  # → HMAC_SECRET
python3 -c 'import secrets; print(secrets.token_urlsafe(24))'  # → ADMIN_TOKEN
```

## 5. Install systemd unit
```bash
cp /opt/wswdy/deploy/dccrime.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now dccrime
systemctl status dccrime
curl -s http://127.0.0.1:8000/healthz   # should print {"status":"ok"}
```

## 6. Logrotate
```bash
cp /opt/wswdy/deploy/logrotate.conf /etc/logrotate.d/dccrime
logrotate -d /etc/logrotate.d/dccrime  # dry-run check
```

## 7. Cloudflare Tunnel
```bash
# Install cloudflared in this LXC
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb -o /tmp/cf.deb
dpkg -i /tmp/cf.deb

# One-time auth
cloudflared tunnel login
cloudflared tunnel create dccrime
# Note the UUID printed.

# Configure
mkdir -p /etc/cloudflared
cp /opt/wswdy/deploy/cloudflared-config.yml.example /etc/cloudflared/config.yml
$EDITOR /etc/cloudflared/config.yml   # fill in UUID
cloudflared tunnel route dns dccrime dccrime.iandmuir.com
cloudflared service install
systemctl enable --now cloudflared
```

## 8. Verify externally
```bash
curl -s https://dccrime.iandmuir.com/healthz
```

## 9. First subscriber
- Visit `https://dccrime.iandmuir.com/`
- Submit your own signup (Ian — `iandmuir@gmail.com`)
- Approve the request via the admin email link
- Tomorrow at 06:00 ET, expect the first digest.
