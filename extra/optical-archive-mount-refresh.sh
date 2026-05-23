#!/bin/sh
set -eu

MOUNT_PATH="${1:-${OPTICAL_ARCHIVE_MOUNT_PATH:-}}"
DEVICE="${2:-${OPTICAL_ARCHIVE_MOUNT_DEVICE:-}}"

if [ -z "${MOUNT_PATH}" ]; then
  echo "mount path is required" >&2
  exit 1
fi

mkdir -p "${MOUNT_PATH}"

if [ -n "${DEVICE}" ]; then
  umount "${MOUNT_PATH}" 2>/dev/null || true
  mount -o ro "${DEVICE}" "${MOUNT_PATH}"
  exit 0
fi

if mountpoint -q "${MOUNT_PATH}"; then
  mount -o remount,ro "${MOUNT_PATH}"
fi
