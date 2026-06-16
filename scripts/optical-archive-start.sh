#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-start}"
ENV_FILE="${OPTICAL_ARCHIVE_ENV_FILE:-/etc/optical-archive/optical-archive.env}"

if [[ -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
fi

OPTICAL_ARCHIVE_HOME="${OPTICAL_ARCHIVE_HOME:-/opt/burnbridge}"
OPTICAL_ARCHIVE_CONFIG_PATH="${OPTICAL_ARCHIVE_CONFIG_PATH:-${OPTICAL_ARCHIVE_HOME}/config/optical-archive.config.json}"
OPTICAL_ARCHIVE_DATA_DIR="${OPTICAL_ARCHIVE_DATA_DIR:-/var/lib/burnbridge}"
OPTICAL_ARCHIVE_LOG_DIR="${OPTICAL_ARCHIVE_LOG_DIR:-/var/log/burnbridge}"
OPTICAL_ARCHIVE_DOTNET_ENV_SCRIPT="${OPTICAL_ARCHIVE_DOTNET_ENV_SCRIPT:-/root/configure.sh}"

RECORDER_DIR="${OPTICAL_ARCHIVE_RECORDER_DIR:-${OPTICAL_ARCHIVE_HOME}/optical-recorder}"
GATEWAY_DIR="${OPTICAL_ARCHIVE_HOME}/versitygw"
RECORDER_EXE="${RECORDER_DIR}/optical-recorder"
GATEWAY_EXE="${GATEWAY_DIR}/versitygw"
RECORDER_PID="${OPTICAL_ARCHIVE_DATA_DIR}/optical-recorder.pid"
GATEWAY_PID="${OPTICAL_ARCHIVE_DATA_DIR}/gateway.pid"

prepare_runtime() {
  mkdir -p \
    "${OPTICAL_ARCHIVE_DATA_DIR}" \
    "${OPTICAL_ARCHIVE_DATA_DIR}/iam" \
    "${OPTICAL_ARCHIVE_LOG_DIR}" \
    "${OPTICAL_ARCHIVE_LOG_DIR}/recorder" \
    "${OPTICAL_ARCHIVE_MOUNT_PATH:-/mnt/optical}"

  if [[ ! -f "${OPTICAL_ARCHIVE_CONFIG_PATH}" ]]; then
    echo "ERROR: config file not found: ${OPTICAL_ARCHIVE_CONFIG_PATH}" >&2
    exit 1
  fi

  if [[ ! -x "${RECORDER_EXE}" ]]; then
    echo "ERROR: recorder executable not found or not executable: ${RECORDER_EXE}" >&2
    exit 1
  fi
  if [[ ! -x "${GATEWAY_EXE}" ]]; then
    echo "ERROR: gateway executable not found or not executable: ${GATEWAY_EXE}" >&2
    exit 1
  fi

  if [[ ! -f "${OPTICAL_ARCHIVE_DATA_DIR}/iam/users.json" && -f "${OPTICAL_ARCHIVE_HOME}/config/users.json" ]]; then
    cp "${OPTICAL_ARCHIVE_HOME}/config/users.json" "${OPTICAL_ARCHIVE_DATA_DIR}/iam/users.json"
  fi
}

load_dotnet_env() {
  if [[ -f "${OPTICAL_ARCHIVE_DOTNET_ENV_SCRIPT}" ]]; then
    # shellcheck disable=SC1090
    source "${OPTICAL_ARCHIVE_DOTNET_ENV_SCRIPT}" >/dev/null 2>&1 || true
  fi
}

recorder_foreground() {
  prepare_runtime
  load_dotnet_env
  export OPTICAL_ARCHIVE_CONFIG_PATH
  export BLOCK_DEVICE_CONFIG_PATH="${BLOCK_DEVICE_CONFIG_PATH:-${OPTICAL_ARCHIVE_CONFIG_PATH}}"
  export BLOCK_DEVICE_LOG_DIR="${BLOCK_DEVICE_LOG_DIR:-${OPTICAL_ARCHIVE_LOG_DIR}/recorder}"
  export ASPNETCORE_URLS="${ASPNETCORE_URLS:-http://0.0.0.0:50051}"

  cd "${RECORDER_DIR}"
  exec "${RECORDER_EXE}"
}

gateway_foreground() {
  prepare_runtime
  export OPTICAL_ARCHIVE_CONFIG_PATH
  export VGW_BURNBRIDGE_ARCHIVE_CONFIG_PATH="${VGW_BURNBRIDGE_ARCHIVE_CONFIG_PATH:-${OPTICAL_ARCHIVE_CONFIG_PATH}}"

  local args=(
    --port "${VGW_PORT:-:7070}"
    --admin-port "${VGW_ADMIN_PORT:-:7080}"
    --access "${ROOT_ACCESS_KEY_ID:-admin}"
    --secret "${ROOT_SECRET_ACCESS_KEY:-admin123456}"
    --region "${VGW_REGION:-us-east-1}"
    --health "${VGW_HEALTH:-/health}"
    --iam-dir "${VGW_IAM_DIR:-${OPTICAL_ARCHIVE_DATA_DIR}/iam}"
  )

  if [[ -n "${VGW_WEBUI_PORT:-:7071}" ]]; then
    args+=(--webui "${VGW_WEBUI_PORT:-:7071}")
  fi
  if [[ "${VGW_WEBUI_NO_TLS:-true}" == "true" ]]; then
    args+=(--webui-no-tls)
  fi
  if [[ -n "${VGW_CORS_ALLOW_ORIGIN:-*}" ]]; then
    args+=(--cors-allow-origin "${VGW_CORS_ALLOW_ORIGIN:-*}")
  fi
  if [[ -n "${VGW_WEBUI_GATEWAYS:-}" ]]; then
    args+=(--webui-gateways "${VGW_WEBUI_GATEWAYS}")
  fi
  if [[ -n "${VGW_WEBUI_ADMIN_GATEWAYS:-}" ]]; then
    args+=(--webui-admin-gateways "${VGW_WEBUI_ADMIN_GATEWAYS}")
  fi

  args+=(
    burnbridge
    --db-path "${VGW_BURNBRIDGE_DB_PATH:-${OPTICAL_ARCHIVE_DATA_DIR}/burnbridge-meta.sqlite}"
    --grpc-addr "${VGW_BURNBRIDGE_GRPC_ADDR:-127.0.0.1:50051}"
    --grpc-dial-timeout "${VGW_BURNBRIDGE_GRPC_DIAL_TIMEOUT:-120s}"
    --grpc-ready-timeout "${VGW_BURNBRIDGE_GRPC_READY_TIMEOUT:-90s}"
    --grpc-ping-timeout "${VGW_BURNBRIDGE_GRPC_PING_TIMEOUT:-60s}"
    --grpc-chunk-size "${VGW_BURNBRIDGE_GRPC_CHUNK_SIZE:-262144}"
    --put-object-timeout "${VGW_BURNBRIDGE_PUT_OBJECT_TIMEOUT:-8h}"
  )

  cd "${GATEWAY_DIR}"
  exec "${GATEWAY_EXE}" "${args[@]}"
}

start_background() {
  prepare_runtime
  stop_background || true
  nohup "$0" recorder >"${OPTICAL_ARCHIVE_DATA_DIR}/optical-recorder.out" 2>&1 &
  echo "$!" >"${RECORDER_PID}"
  sleep 3
  nohup "$0" gateway >"${OPTICAL_ARCHIVE_DATA_DIR}/gateway.out" 2>&1 &
  echo "$!" >"${GATEWAY_PID}"
  status
}

stop_pid_file() {
  local pid_file="$1"
  if [[ -f "${pid_file}" ]]; then
    local pid
    pid="$(cat "${pid_file}" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
      sleep 1
      kill -9 "${pid}" 2>/dev/null || true
    fi
    rm -f "${pid_file}"
  fi
}

stop_background() {
  stop_pid_file "${GATEWAY_PID}"
  stop_pid_file "${RECORDER_PID}"
  pkill -f "${GATEWAY_EXE}" 2>/dev/null || true
  pkill -f "${RECORDER_EXE}" 2>/dev/null || true
}

status() {
  echo "Optical Archive status"
  echo "  home    : ${OPTICAL_ARCHIVE_HOME}"
  echo "  config  : ${OPTICAL_ARCHIVE_CONFIG_PATH}"
  echo "  s3      : http://127.0.0.1${VGW_PORT:-:7070}"
  echo "  admin   : http://127.0.0.1${VGW_ADMIN_PORT:-:7080}"
  echo "  webui   : http://127.0.0.1${VGW_WEBUI_PORT:-:7071}"
  pgrep -af "${RECORDER_EXE}|${GATEWAY_EXE}" || true
}

case "${ACTION}" in
  recorder)
    recorder_foreground
    ;;
  gateway)
    gateway_foreground
    ;;
  start)
    start_background
    ;;
  stop)
    stop_background
    ;;
  restart)
    stop_background
    start_background
    ;;
  status)
    status
    ;;
  logs)
    tail -n 120 -f "${OPTICAL_ARCHIVE_DATA_DIR}/optical-recorder.out" "${OPTICAL_ARCHIVE_DATA_DIR}/gateway.out"
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs|recorder|gateway}" >&2
    exit 2
    ;;
esac
