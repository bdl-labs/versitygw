#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_SCRIPT="${OPTICAL_ARCHIVE_START_SCRIPT:-}"

if [[ -z "${START_SCRIPT}" ]]; then
  if [[ -x "${SCRIPT_DIR}/optical-archive-start.sh" ]]; then
    START_SCRIPT="${SCRIPT_DIR}/optical-archive-start.sh"
  else
    START_SCRIPT="/opt/burnbridge/scripts/optical-archive-start.sh"
  fi
fi

echo "Stopping optical archive gateway and recorder..."

if [[ -x "${START_SCRIPT}" ]]; then
  "${START_SCRIPT}" stop || true
fi

pkill -f '/opt/burnbridge/versitygw/versitygw' 2>/dev/null || true
pkill -f '/opt/burnbridge/burnserver/BurnServer' 2>/dev/null || true
pkill -f 'BurnServer.dll' 2>/dev/null || true

echo "Stopped."
