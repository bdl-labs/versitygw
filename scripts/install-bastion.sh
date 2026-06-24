#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXTRA_DIR="${BASTION_EXTRA_DIR:-$(cd "${SCRIPT_DIR}/../extra/bastion" && pwd)}"
BACKUP_ROOT="${BASTION_BACKUP_ROOT:-/var/backups/burnbridge-bastion}"
TRUSTED_CIDR="${BASTION_TRUSTED_CIDR:-192.168.2.0/24}"
BASTION_USER="${BASTION_USER:-bastion}"
COCKPIT_IDLE_TIMEOUT="${BASTION_COCKPIT_IDLE_TIMEOUT:-900}"

PREPARE_ONLY="false"
ENABLE_FIREWALL="false"
DISABLE_SSH="false"
LOCK_ROOT="false"
CREATE_USER="false"
ENFORCE_MFA="false"
INSTALL_PACKAGES="true"
SKIP_APT_UPDATE="false"

usage() {
  cat <<EOF
Usage: install-bastion.sh [options]

Safe staged installer for the BurnBridge Raspberry Pi web bastion.

Options:
  --prepare-only       Install packages and write configs only. Does not enable UFW,
                       disable SSH, or lock root. Recommended first run.
  --create-user        Create ${BASTION_USER} and add it to sudo.
  --enable-firewall    Enable UFW with SSH still allowed from ${TRUSTED_CIDR}.
  --disable-ssh        Disable SSH services. Use only after Cockpit HTTPS login works.
  --lock-root          Lock root password. Use only after deployment no longer uses root SSH.
  --enforce-mfa        Require Google Authenticator for Cockpit. Without this, nullok is used.
  --skip-packages      Do not run apt install.
  --skip-apt-update    Install packages using current apt indexes.
  -h, --help           Show this help.

Environment:
  BASTION_TRUSTED_CIDR=${TRUSTED_CIDR}
  BASTION_USER=${BASTION_USER}
  BASTION_COCKPIT_IDLE_TIMEOUT=${COCKPIT_IDLE_TIMEOUT}
EOF
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "ERROR: this command must be run as root." >&2
    exit 1
  fi
}

backup_file() {
  local path="$1"
  local backup_dir="$2"
  if [[ -e "${path}" || -L "${path}" ]]; then
    mkdir -p "${backup_dir}$(dirname "${path}")"
    cp -a "${path}" "${backup_dir}${path}"
  fi
}

install_file() {
  local src="$1"
  local dst="$2"
  local mode="$3"
  install -D -m "${mode}" "${src}" "${dst}"
}

ensure_caddy_certificate() {
  local cert="/etc/caddy/burnbridge-bastion.crt"
  local key="/etc/caddy/burnbridge-bastion.key"
  local primary_ip

  if [[ -s "${cert}" && -s "${key}" ]]; then
    return
  fi

  if ! command -v openssl >/dev/null 2>&1; then
    echo "WARN: openssl is missing; Caddy certificate generation skipped." >&2
    return
  fi

  primary_ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
  if [[ -z "${primary_ip}" ]]; then
    primary_ip="127.0.0.1"
  fi

  openssl req -x509 -nodes -newkey rsa:2048 -days 825 \
    -keyout "${key}" \
    -out "${cert}" \
    -subj "/CN=burnbridge-bastion" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:${primary_ip}" >/dev/null 2>&1

  chown root:caddy "${cert}" "${key}" 2>/dev/null || true
  chmod 0644 "${cert}"
  chmod 0640 "${key}"
}

ensure_pam_google_authenticator() {
  local pam_file="/etc/pam.d/cockpit"
  local backup_dir="$1"
  local module_line

  if [[ ! -f "${pam_file}" ]]; then
    echo "WARN: ${pam_file} does not exist; skipping Cockpit PAM MFA configuration. Install cockpit first." >&2
    return
  fi

  backup_file "${pam_file}" "${backup_dir}"
  if [[ "${ENFORCE_MFA}" == "true" ]]; then
    module_line="auth required pam_google_authenticator.so"
  else
    module_line="auth required pam_google_authenticator.so nullok"
  fi

  if ! grep -q 'pam_google_authenticator\.so' "${pam_file}"; then
    printf '%s\n' "${module_line}" | cat - "${pam_file}" > "${pam_file}.tmp"
    mv "${pam_file}.tmp" "${pam_file}"
    return
  fi

  sed -i "s#^auth .*pam_google_authenticator\\.so.*#${module_line}#" "${pam_file}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prepare-only)
      PREPARE_ONLY="true"
      ;;
    --create-user)
      CREATE_USER="true"
      ;;
    --enable-firewall)
      ENABLE_FIREWALL="true"
      ;;
    --disable-ssh)
      DISABLE_SSH="true"
      ;;
    --lock-root)
      LOCK_ROOT="true"
      ;;
    --enforce-mfa)
      ENFORCE_MFA="true"
      ;;
    --skip-packages)
      INSTALL_PACKAGES="false"
      ;;
    --skip-apt-update)
      SKIP_APT_UPDATE="true"
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

if [[ "${PREPARE_ONLY}" == "true" ]]; then
  ENABLE_FIREWALL="false"
  DISABLE_SSH="false"
  LOCK_ROOT="false"
fi

require_root

timestamp="$(date +%Y%m%d-%H%M%S)"
backup_dir="${BACKUP_ROOT}/${timestamp}"
mkdir -p "${backup_dir}"

echo "Creating bastion backup at ${backup_dir}"
backup_file /etc/caddy/Caddyfile "${backup_dir}"
backup_file /etc/cockpit/cockpit.conf "${backup_dir}"
backup_file /etc/pam.d/cockpit "${backup_dir}"
backup_file /etc/fail2ban/jail.d/burnbridge-cockpit.local "${backup_dir}"
backup_file /etc/fail2ban/filter.d/burnbridge-cockpit.conf "${backup_dir}"
backup_file /etc/systemd/system/cockpit.socket.d/burnbridge-listen-localhost.conf "${backup_dir}"

if [[ "${INSTALL_PACKAGES}" == "true" ]]; then
  if [[ "${SKIP_APT_UPDATE}" != "true" ]]; then
    apt-get update
  fi
  apt-get install -y cockpit caddy fail2ban ufw libpam-google-authenticator
fi

install_file "${EXTRA_DIR}/Caddyfile.cockpit" /etc/caddy/Caddyfile 0644
ensure_caddy_certificate
install_file "${EXTRA_DIR}/cockpit.conf" /etc/cockpit/cockpit.conf 0644
sed -i "s/^IdleTimeout=.*/IdleTimeout=${COCKPIT_IDLE_TIMEOUT}/" /etc/cockpit/cockpit.conf
install_file "${EXTRA_DIR}/cockpit.socket.override.conf" /etc/systemd/system/cockpit.socket.d/burnbridge-listen-localhost.conf 0644
install_file "${EXTRA_DIR}/fail2ban-cockpit.local" /etc/fail2ban/jail.d/burnbridge-cockpit.local 0644
install_file "${EXTRA_DIR}/fail2ban-burnbridge-cockpit.conf" /etc/fail2ban/filter.d/burnbridge-cockpit.conf 0644
ensure_pam_google_authenticator "${backup_dir}"

if [[ "${CREATE_USER}" == "true" ]] && ! id "${BASTION_USER}" >/dev/null 2>&1; then
  adduser "${BASTION_USER}"
  usermod -aG sudo "${BASTION_USER}"
fi

systemctl daemon-reload
systemctl enable cockpit.socket caddy fail2ban
systemctl restart cockpit.socket
systemctl restart caddy
systemctl restart fail2ban

if [[ "${ENABLE_FIREWALL}" == "true" ]]; then
  ufw default deny incoming
  ufw default allow outgoing
  ufw allow 443/tcp
  ufw allow from "${TRUSTED_CIDR}" to any port 22 proto tcp
  ufw allow from "${TRUSTED_CIDR}" to any port 7070 proto tcp
  ufw allow from "${TRUSTED_CIDR}" to any port 7071 proto tcp
  ufw allow from "${TRUSTED_CIDR}" to any port 7080 proto tcp
  ufw deny 50051/tcp
  ufw --force enable
fi

if [[ "${DISABLE_SSH}" == "true" ]]; then
  systemctl stop ssh sshd 2>/dev/null || true
  systemctl disable ssh sshd 2>/dev/null || true
fi

if [[ "${LOCK_ROOT}" == "true" ]]; then
  passwd -l root
fi

cat <<EOF
Bastion install stage completed.

Backup:
  ${backup_dir}

Verify:
  /opt/burnbridge/scripts/bastion-status.sh
  ss -lntp | grep -E ':22|:443|:9090|:7070|:7071|:7080|:50051'
  curl -k https://127.0.0.1/
  curl -fsS http://127.0.0.1:7070/health

MFA enrollment:
  sudo -iu ${BASTION_USER} google-authenticator

Rollback:
  /opt/burnbridge/scripts/uninstall-bastion.sh --keep-packages
EOF
