#!/usr/bin/env bash
set -euo pipefail

MOUNT_PATH="${1:-${OPTICAL_ARCHIVE_MOUNT_PATH:-/mnt/optical}}"
DEVICE="${2:-${OPTICAL_ARCHIVE_MOUNT_DEVICE:-/dev/sr0}}"
LOCK_FILE="${OPTICAL_ARCHIVE_MOUNT_LOCK_FILE:-/tmp/optical-archive-mount-refresh.lock}"
REUSE_EXISTING="${OPTICAL_ARCHIVE_MOUNT_REUSE_EXISTING:-false}"
UMOUNT_RETRIES="${OPTICAL_ARCHIVE_MOUNT_UMOUNT_RETRIES:-5}"
UMOUNT_RETRY_DELAY_SECONDS="${OPTICAL_ARCHIVE_MOUNT_UMOUNT_RETRY_DELAY_SECONDS:-1}"
LAZY_UMOUNT_FALLBACK="${OPTICAL_ARCHIVE_MOUNT_LAZY_UMOUNT_FALLBACK:-true}"

if [[ -z "${MOUNT_PATH}" ]]; then
  echo "mount path is required" >&2
  exit 1
fi

mkdir -p "${MOUNT_PATH}"

exec 9>"${LOCK_FILE}"
flock 9

if [[ "${REUSE_EXISTING}" == "true" && -n "${DEVICE}" && -b "${DEVICE}" ]] && mountpoint -q "${MOUNT_PATH}"; then
  mounted_source="$(findmnt -n -o SOURCE --target "${MOUNT_PATH}" 2>/dev/null || true)"
  if [[ "${mounted_source}" == "${DEVICE}" ]]; then
    echo "optical mount already active: ${DEVICE} -> ${MOUNT_PATH}"
    exit 0
  fi
fi

while mountpoint -q "${MOUNT_PATH}"; do
  unmounted="false"
  for ((attempt = 1; attempt <= UMOUNT_RETRIES; attempt++)); do
    if umount "${MOUNT_PATH}" 2>/dev/null; then
      unmounted="true"
      break
    fi
    sleep "${UMOUNT_RETRY_DELAY_SECONDS}"
  done

  if [[ "${unmounted}" == "true" ]]; then
    continue
  fi

  if [[ "${LAZY_UMOUNT_FALLBACK}" == "true" ]]; then
    umount -l "${MOUNT_PATH}" 2>/dev/null || {
      echo "failed to lazy-unmount existing optical mount: ${MOUNT_PATH}" >&2
      exit 1
    }
    break
  fi

  echo "failed to unmount existing optical mount: ${MOUNT_PATH}" >&2
  exit 1
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
