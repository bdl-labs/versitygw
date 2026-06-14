#!/usr/bin/env bash
set -euo pipefail

MOUNT_PATH="${1:-${OPTICAL_ARCHIVE_MOUNT_PATH:-/mnt/optical}}"
DEVICE="${2:-${OPTICAL_ARCHIVE_MOUNT_DEVICE:-/dev/sr0}}"

if [[ -z "${MOUNT_PATH}" ]]; then
  echo "mount path is required" >&2
  exit 1
fi

mkdir -p "${MOUNT_PATH}"

if mountpoint -q "${MOUNT_PATH}"; then
  umount "${MOUNT_PATH}" 2>/dev/null || true
fi

if [[ -n "${DEVICE}" && -b "${DEVICE}" ]]; then
  mount -o ro "${DEVICE}" "${MOUNT_PATH}"
  exit 0
fi

echo "mount device is missing or not a block device: ${DEVICE}" >&2
exit 1
