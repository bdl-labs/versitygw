# BurnBridge Production Checklist

Date: 2026-06-17

Use this checklist before treating a deployment as production-ready.

## Startup

- Start `optical-recorder`.
- Confirm recorder log contains `Recorder SQLite integrity check passed`.
- Start `versitygw burnbridge`.
- Confirm gateway log contains `sqlite safety backup created`.
- Confirm no `sqlite integrity check failed` message appears.

## Empty / Blank Disc

- Insert a blank disc.
- Run `drive-info` against the control bucket.
- Run `disc-info` against the control bucket.
- Confirm the data bucket is the current disc bucket only.
- Confirm no previous disc bucket appears in `ListBuckets`.

## Upload And Finalize

- Upload at least one small file with direct PUT.
- Upload at least one large file with multipart upload.
- Confirm `ListObjects`, `HeadObject`, and object metadata are correct.
- Run `finalize-layout`.
- Confirm the recorder writes `OA<bucket>.sqlite3` to the disc layout.
- Run download verification for every uploaded object.

## Media Change

- Run `media-removed` before opening/removing media on Linux deployments.
- Confirm `ListBuckets` returns only the control bucket after removal.
- Insert the same disc and run `media-inserted`.
- Confirm the original data bucket and object list return.
- Insert a different disc and run `media-inserted`.
- Confirm old bucket metadata is pruned and no stale objects appear.

## Close Disc

- Run `close-disc`.
- Confirm `disc-info` reports finalized/read-only media.
- Run `optical-recovery-check` with recorder `udf-layout.db`, recorder journal, and mounted `OA<bucket>.sqlite3`.
- Confirm the recovery verdict is `SAFE_FINALIZED`.
- Confirm final download verification passes.
- Only after this point may local runtime DB snapshots and recovery journal files be archived or cleaned.

## Failure Drill

- Kill only the gateway process and restart it.
- Kill only the recorder process and restart it.
- Confirm both services pass SQLite integrity checks at startup.
- Confirm the data bucket either reloads correctly or remains hidden if no disc is present.

## Files To Preserve After Any Failure

- Recorder `udf-layout.db`
- Recorder `runtime-snapshots`
- Recorder `media-change-backups`
- Recorder `recovery-journal`
- Gateway `burnbridge-meta.db`
- Gateway `disc-backups`
- Gateway `sqlite-safety-backups`

Do not delete these files until the disc is finalized and full download verification succeeds.

## Recovery Verdict Command

Example:

```bash
optical-recovery-check \
  --layout-db /var/lib/burnbridge/recorder/udf-layout.db \
  --mounted-db /mnt/optical/OA<bucket>.sqlite3 \
  --journal /var/lib/burnbridge/recorder/recovery-journal/recorder-events.jsonl \
  --snapshots-dir /var/lib/burnbridge/recorder/runtime-snapshots \
  --media-backups-dir /var/lib/burnbridge/recorder/media-change-backups
```

Exit codes:

- `0`: finalized metadata is consistent.
- `1`: needs finalize, mounted metadata, or manual review.
- `2`: recovery is required.
- `3`: analysis is blocked, usually because local metadata is missing or corrupt.
