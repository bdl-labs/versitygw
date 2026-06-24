#!/usr/bin/env bash
set -euo pipefail

BACKUP_ROOT="${BASTION_BACKUP_ROOT:-/var/backups/burnbridge-bastion}"
TRUSTED_CIDR="${BASTION_TRUSTED_CIDR:-192.168.2.0/24}"

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "ERROR: this command must be run as root." >&2
    exit 1
  fi
}

usage() {
  cat <<EOF
Usage: uninstall-bastion.sh [--keep-packages]

Restores SSH access and disables the web bastion services. It keeps system file
backups under ${BACKUP_ROOT}.

Options:
  --keep-packages   Do not remove installed packages.
EOF
}

KEEP_PACKAGES="false"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-packages)
      KEEP_PACKAGES="true"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

require_root

systemctl enable ssh sshd 2>/dev/null || true
systemctl start ssh sshd 2>/dev/null || true

if command -v ufw >/dev/null 2>&1; then
  ufw allow from "${TRUSTED_CIDR}" to any port 22 proto tcp || true
  ufw reload || true
fi

systemctl disable --now caddy cockpit.socket fail2ban 2>/dev/null || true
rm -f /etc/systemd/system/cockpit.socket.d/burnbridge-listen-localhost.conf
systemctl daemon-reload

if [[ "${KEEP_PACKAGES}" != "true" ]]; then
  apt-get remove -y caddy cockpit fail2ban libpam-google-authenticator ufw || true
fi

echo "Bastion rollback completed. SSH should be available from ${TRUSTED_CIDR}."
echo "Backups were kept under ${BACKUP_ROOT}."
