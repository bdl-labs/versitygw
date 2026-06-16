#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-}"
SERVICE_DIR="${SERVICE_DIR:-/etc/systemd/system}"
CONFIG_DIR="${CONFIG_DIR:-/etc/optical-archive}"
OPTICAL_ARCHIVE_HOME="${OPTICAL_ARCHIVE_HOME:-/opt/burnbridge}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXTRA_DIR="${OPTICAL_ARCHIVE_EXTRA_DIR:-$(cd "${SCRIPT_DIR}/../extra" && pwd)}"

services=(optical-archive-recorder.service optical-archive-gateway.service)
all_services=(optical-archive-recorder.service optical-archive-gateway.service optical-archive-mount-refresh.service)

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "ERROR: this command must be run as root." >&2
    exit 1
  fi
}

install_files() {
  require_root
  mkdir -p "${OPTICAL_ARCHIVE_HOME}/scripts" "${CONFIG_DIR}"

  install -m 0755 "${SCRIPT_DIR}/optical-archive-start.sh" "${OPTICAL_ARCHIVE_HOME}/scripts/optical-archive-start.sh"
  install -m 0755 "${SCRIPT_DIR}/optical-archive-mount-refresh.sh" "${OPTICAL_ARCHIVE_HOME}/scripts/optical-archive-mount-refresh.sh"
  install -m 0755 "${SCRIPT_DIR}/stop-all.sh" "${OPTICAL_ARCHIVE_HOME}/scripts/stop-all.sh"
  install -m 0755 "${SCRIPT_DIR}/optical-archive-service.sh" "${OPTICAL_ARCHIVE_HOME}/scripts/optical-archive-service.sh"
  install -m 0644 "${EXTRA_DIR}/optical-archive-recorder.service" "${SERVICE_DIR}/optical-archive-recorder.service"
  install -m 0644 "${EXTRA_DIR}/optical-archive-gateway.service" "${SERVICE_DIR}/optical-archive-gateway.service"
  install -m 0644 "${EXTRA_DIR}/optical-archive-mount-refresh.service" "${SERVICE_DIR}/optical-archive-mount-refresh.service"

  if [[ ! -f "${CONFIG_DIR}/optical-archive.env" ]]; then
    install -m 0644 "${EXTRA_DIR}/optical-archive.env.example" "${CONFIG_DIR}/optical-archive.env"
  fi

  systemctl daemon-reload
  systemctl enable "${services[@]}"
  echo "Installed optical archive services."
  echo "Edit ${CONFIG_DIR}/optical-archive.env if this node uses different ports, paths, or credentials."
}

uninstall_files() {
  require_root
  systemctl stop "${services[@]}" 2>/dev/null || true
  systemctl disable "${services[@]}" 2>/dev/null || true
  rm -f "${SERVICE_DIR}/optical-archive-recorder.service" \
        "${SERVICE_DIR}/optical-archive-gateway.service" \
        "${SERVICE_DIR}/optical-archive-mount-refresh.service"
  systemctl daemon-reload
  echo "Uninstalled optical archive services. Runtime data and ${CONFIG_DIR}/optical-archive.env were kept."
}

start_services() {
  require_root
  systemctl start optical-archive-recorder.service
  systemctl start optical-archive-gateway.service
}

stop_services() {
  require_root
  systemctl stop optical-archive-gateway.service 2>/dev/null || true
  systemctl stop optical-archive-recorder.service 2>/dev/null || true
}

case "${ACTION}" in
  install)
    install_files
    ;;
  uninstall)
    uninstall_files
    ;;
  start)
    start_services
    ;;
  stop)
    stop_services
    ;;
  restart)
    stop_services
    start_services
    ;;
  status)
    systemctl status "${all_services[@]}" --no-pager
    ;;
  enable)
    require_root
    systemctl enable "${services[@]}"
    ;;
  disable)
    require_root
    systemctl disable "${services[@]}"
    ;;
  logs)
    journalctl -u optical-archive-recorder.service -u optical-archive-gateway.service -f
    ;;
  *)
    cat >&2 <<'EOF'
Usage: optical-archive-service.sh {install|uninstall|start|stop|restart|status|enable|disable|logs}

Examples:
  sudo /opt/burnbridge/scripts/optical-archive-service.sh install
  sudo /opt/burnbridge/scripts/optical-archive-service.sh start
  sudo /opt/burnbridge/scripts/optical-archive-service.sh status
  sudo /opt/burnbridge/scripts/optical-archive-service.sh stop
  sudo /opt/burnbridge/scripts/optical-archive-service.sh uninstall
EOF
    exit 2
    ;;
esac
