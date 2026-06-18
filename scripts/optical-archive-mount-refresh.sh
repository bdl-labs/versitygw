#!/usr/bin/env bash
set -euo pipefail

MOUNT_PATH="${1:-${OPTICAL_ARCHIVE_MOUNT_PATH:-/mnt/optical}}"
DEVICE="${2:-${OPTICAL_ARCHIVE_MOUNT_DEVICE:-/dev/sr0}}"
LOCK_FILE="${OPTICAL_ARCHIVE_MOUNT_LOCK_FILE:-/tmp/optical-archive-mount-refresh.lock}"

if [[ -z "${MOUNT_PATH}" ]]; then
  echo "mount path is required" >&2
  exit 1
fi

mkdir -p "${MOUNT_PATH}"

exec 9>"${LOCK_FILE}"
flock 9

while mountpoint -q "${MOUNT_PATH}"; do
  umount "${MOUNT_PATH}" 2>/dev/null || {
    echo "failed to unmount existing optical mount: ${MOUNT_PATH}" >&2
    exit 1
  }
done

if [[ -n "${DEVICE}" && -b "${DEVICE}" ]]; then
  if mountpoint -q "${MOUNT_PATH}"; then
    echo "refusing to mount over existing optical mount: ${MOUNT_PATH}" >&2
    exit 1
  fi

  mount -o ro "${DEVICE}" "${MOUNT_PATH}"
  mount_count="$(findmnt -R "${MOUNT_PATH}" 2>/dev/null | tail -n +2 | wc -l | tr -d ' ')"
  if [[ "${mount_count}" != "1" ]]; then
    echo "unexpected mount nesting after refresh: ${mount_count}" >&2
    exit 1
  fi
  exit 0
fi

echo "mount device is missing or not a block device: ${DEVICE}" >&2
exit 1
