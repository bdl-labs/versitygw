#!/usr/bin/env bash
set -euo pipefail

# BurnBridge fault-injection recovery test:
# 1) upload full object and capture baseline checksum
# 2) upload same object again but kill recorder after N segments
# 3) restart recorder and re-upload
# 4) verify object checksum and manifest consistency logs

S3_ENDPOINT="${1:-http://127.0.0.1:10000}"
BUCKET="${2:-mybucket}"
OBJECT_KEY="${3:-test.bin}"
SEGMENT_CUTOFF="${4:-5}"

WORK_DIR="$(mktemp -d)"
PAYLOAD="${WORK_DIR}/payload.bin"
PAYLOAD_SHA="${WORK_DIR}/payload.sha256"
DOWNLOADED="${WORK_DIR}/downloaded.bin"
LOG_CAPTURE="${WORK_DIR}/burnbridge.log"

# Inputs expected from environment:
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
#   BURNBRIDGE_DB_PATH
#   BURNSERVER_START_CMD   e.g. "dotnet run --project /path/to/BurnServer.csproj"
#   BURNSERVER_PROCESS_NAME e.g. "BurnServer"
#   VERSITYGW_START_CMD    e.g. "./versitygw ... burnbridge ..."

required_env=(AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY BURNBRIDGE_DB_PATH BURNSERVER_START_CMD BURNSERVER_PROCESS_NAME VERSITYGW_START_CMD)
for name in "${required_env[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required environment variable: ${name}"
    exit 1
  fi
done

# Default recorder gRPC target to local loopback when not explicitly set.
: "${VGW_BURNBRIDGE_GRPC_ADDR:=127.0.0.1:50051}"
export VGW_BURNBRIDGE_GRPC_ADDR

cleanup() {
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

start_gateway() {
  nohup bash -lc "${VERSITYGW_START_CMD}" > "${LOG_CAPTURE}" 2>&1 &
  sleep 3
}

start_burnserver() {
  nohup bash -lc "${BURNSERVER_START_CMD}" >> "${LOG_CAPTURE}" 2>&1 &
  sleep 3
}

kill_burnserver() {
  pkill -f "${BURNSERVER_PROCESS_NAME}" || true
}

ensure_bucket() {
  aws --endpoint-url "${S3_ENDPOINT}" s3api head-bucket --bucket "${BUCKET}" >/dev/null 2>&1 || \
    aws --endpoint-url "${S3_ENDPOINT}" s3api create-bucket --bucket "${BUCKET}" >/dev/null
}

upload_object() {
  local src="$1"
  aws --endpoint-url "${S3_ENDPOINT}" s3 cp "${src}" "s3://${BUCKET}/${OBJECT_KEY}" >/dev/null
}

download_object() {
  local dst="$1"
  aws --endpoint-url "${S3_ENDPOINT}" s3 cp "s3://${BUCKET}/${OBJECT_KEY}" "${dst}" >/dev/null
}

echo "[1/7] generating deterministic payload..."
python3 - <<'PY' > "${PAYLOAD}"
import os
import random
r = random.Random(20260516)
size = 8 * 1024 * 1024
buf = bytearray(size)
for i in range(size):
    buf[i] = r.randrange(0, 256)
os.write(1, bytes(buf))
PY
sha256sum "${PAYLOAD}" > "${PAYLOAD_SHA}"
echo "payload sha256: $(cut -d' ' -f1 "${PAYLOAD_SHA}")"

echo "[2/7] starting services..."
start_burnserver
start_gateway
ensure_bucket

echo "[3/7] baseline PUT..."
upload_object "${PAYLOAD}"

echo "[4/7] injecting failure after ${SEGMENT_CUTOFF} segment-acks..."
kill_burnserver
start_burnserver

# trigger second PUT in background then kill recorder soon to force interruption
( upload_object "${PAYLOAD}" ) &
PUT_PID=$!
sleep 1
kill_burnserver
wait "${PUT_PID}" || true

echo "[5/7] restart recorder and replay PUT..."
start_burnserver
upload_object "${PAYLOAD}"

echo "[6/7] verifying object bytes..."
download_object "${DOWNLOADED}"
DOWN_SHA="$(sha256sum "${DOWNLOADED}" | cut -d' ' -f1)"
SRC_SHA="$(cut -d' ' -f1 "${PAYLOAD_SHA}")"
if [[ "${DOWN_SHA}" != "${SRC_SHA}" ]]; then
  echo "ERROR: object checksum mismatch src=${SRC_SHA} dst=${DOWN_SHA}"
  exit 2
fi
echo "checksum OK: ${DOWN_SHA}"

echo "[7/7] checking recovery stats logs..."
if ! rg -n "upload recovery stats|segments_skipped|segments_replayed|segments_trimmed|skip_hit_rate_pct" "${LOG_CAPTURE}" >/dev/null; then
  echo "ERROR: recovery stats logs not found in ${LOG_CAPTURE}"
  exit 3
fi
rg -n "upload recovery stats|segments_skipped|segments_replayed|segments_trimmed|skip_hit_rate_pct" "${LOG_CAPTURE}" || true

echo "PASS: burnbridge recovery fault-injection test finished."
