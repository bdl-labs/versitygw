#!/usr/bin/env bash
set -euo pipefail

echo "== services =="
systemctl is-active cockpit.socket caddy fail2ban ufw ssh sshd 2>/dev/null || true

echo
echo "== listeners =="
ss -lntp | grep -E ':22|:443|:9090|:7070|:7071|:7080|:50051' || true

echo
echo "== firewall =="
if command -v ufw >/dev/null 2>&1; then
  ufw status verbose || true
else
  echo "ufw is not installed"
fi

echo
echo "== local checks =="
curl -kfsS --max-time 5 https://127.0.0.1/ >/dev/null && echo "cockpit via caddy: OK" || echo "cockpit via caddy: FAIL"
curl -fsS --max-time 5 http://127.0.0.1:7070/health >/dev/null && echo "burnbridge s3 health: OK" || echo "burnbridge s3 health: FAIL"
