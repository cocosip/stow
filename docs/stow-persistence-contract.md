# Stow 1.0 Persistence And Recovery Contract

## 1. Purpose

This document fixes the Stow 1.0 disk layout, SQLite schemas, journal frames,
projection rules, and crash-recovery invariants. Implementations may optimize
internal algorithms but must preserve the ordering, idempotency, and recovery
conclusions defined here.

Stow does not promise interoperability with Locus disk formats. Stow 1.x
formats must remain backward-readable; an incompatible change requires a new
schema or format version and an explicit migration.

## 2. Directory Layout

```text
{metadataDirectory}/
  tenants.json
  {tenantId}/metadata.db

{quotaDirectory}/
  {tenantId}/quotas.db

{queueDirectory}/
  {tenantId}/
    queue.log
    queue.state.json
    projector.cursor.json
    projection.snapshot.json

{watcherDirectory}/
  options.json
  root.json
  source-cleanup.db
  watchers/{watcherId}.json
  history/{watcherId}.jsonl

{volumeMount}/
  {tenantId}/{shard...}/{fileKey}{extension}
  .deadletter/{tenantId}/{yyyyMMdd}/{shard...}/{fileKey}{extension}
```

JSON state files use UTF-8, LF, ISO-8601 UTC timestamps, and camelCase fields.
Persistent updates use a same-directory temporary file, force, and atomic
replacement; supported platforms also force the parent directory. Temporary
files are named `.{name}.{uuid}.tmp`. Startup safely removes only unreferenced
temporary files that match this pattern.

### 2.1 tenants.json Version 1

`{metadataDirectory}/tenants.json` is the sole durable tenant-lifecycle
document. Version 1 uses exactly these camelCase fields:

```json
{
  "schemaVersion": 1,
  "tenants": [
    {
      "tenantId": "tenant-a",
      "status": "ENABLED",
      "createdAt": "2026-09-18T00:00:00Z",
      "updatedAt": "2026-09-18T00:00:00Z",
      "maxFiles": 0
    }
  ]
}
```

- `schemaVersion` is integer `1`; unknown versions or fields are
  forward-incompatible and reject reads and writes while preserving the file.
- `tenants` is an array of unique tenant IDs persisted in ascending order.
- `tenantId` follows the restricted identifier rules below; `status` is
  `ENABLED` or `DISABLED`.
- `createdAt` and `updatedAt` are UTC instants, with updated not before created.
- `maxFiles` is non-negative; `0` means unlimited. The creation-time default
  is copied and later default changes do not rewrite existing entries.

Each update uses `.{name}.{uuid}.tmp`. Constructing `JsonTenantRepository`
constitutes tenant-state startup: before reading the document it removes only
regular temporary files matching the exact `tenants.json` and UUID pattern.
Other files, directories, and links remain untouched. A failure after forcing
the temporary file but before replacement must leave the committed document
readable.

## 3. Identifiers And Paths

- `fileKey`: 32 lowercase hexadecimal characters from a secure 128-bit value.
- `eventId` and `leaseId`: UUID v4, lowercase hyphenated JSON form.
- Tenant, watcher, and volume IDs: 1-128 ASCII letters, digits, `.`, `_`, or
  `-`, excluding `.`, `..`, surrounding whitespace, and Windows device names.
- Logical directories use `/`; empty values normalize to `/`; reject `..`,
  backslashes, NUL, and control characters.
- Extensions come from the last original-name segment, include the leading
  dot, are at most 32 characters, and allow only letters, digits, `.`, `_`,
  and `-`. Invalid extensions are discarded rather than concatenated.

After creation, paths are normalized and verified to remain under their owning
root. Symbolic links are not followed by default.

## 4. SQLite Common Rules

Metadata and quota use independent databases per tenant. Open connections run:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-4000;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;
PRAGMA temp_store=MEMORY;
```

Configurable values use allowlists or numeric range validation and are never
concatenated into SQL. Time columns store UTC epoch milliseconds. Booleans use
`INTEGER NOT NULL CHECK(value IN (0,1))`. Schema version is
`PRAGMA user_version`, set to version 1 in 1.0.

## 5. metadata.db Schema Version 1

```sql
CREATE TABLE files (
    file_key                    TEXT PRIMARY KEY NOT NULL,
    tenant_id                   TEXT NOT NULL,
    volume_id                   TEXT NOT NULL,
    physical_path               TEXT NOT NULL,
    logical_directory           TEXT NOT NULL,
    file_size                   INTEGER NOT NULL CHECK(file_size >= 0),
    created_at_ms               INTEGER NOT NULL,
    status                      INTEGER NOT NULL,
    retry_count                 INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
    last_failed_at_ms           INTEGER,
    last_error                  TEXT,
    lease_id                    TEXT,
    processing_started_at_ms    INTEGER,
    completed_at_ms             INTEGER,
    delete_succeeded_at_ms      INTEGER,
    dead_lettered_at_ms         INTEGER,
    available_at_ms             INTEGER,
    original_file_name          TEXT,
    file_extension              TEXT,
    metadata_json               TEXT,
    import_operation_id         TEXT,
    last_event_sequence         INTEGER NOT NULL,
    row_version                 INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE applied_events (
    event_id                    TEXT PRIMARY KEY NOT NULL,
    sequence_number             INTEGER NOT NULL UNIQUE,
    applied_at_ms               INTEGER NOT NULL
);

CREATE INDEX idx_files_status_available
    ON files(status, available_at_ms, created_at_ms, file_key);
CREATE INDEX idx_files_status_completed
    ON files(status, completed_at_ms, file_key);
CREATE INDEX idx_files_status_failed
    ON files(status, last_failed_at_ms, file_key);
CREATE UNIQUE INDEX idx_files_physical_path
    ON files(physical_path);
CREATE UNIQUE INDEX idx_files_import_operation
    ON files(tenant_id, import_operation_id)
    WHERE import_operation_id IS NOT NULL;
```

Each projection batch is one transaction: check `applied_events`, apply the
reducer, then insert the event ID. A duplicate event ID is idempotent success;
the same sequence with a different event ID is a fact conflict and stops that
tenant's projection.

Claiming uses a conditional update. Candidate selection and
`UPDATE ... WHERE row_version=? AND status IN (...)` share one transaction.
Success updates lease, processing start, status, and row version. An affected
row count other than one means a race and causes another candidate selection.

## 6. quotas.db Schema Version 1

```sql
CREATE TABLE tenant_quota (
    singleton_id                INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    current_count               INTEGER NOT NULL DEFAULT 0 CHECK(current_count >= 0),
    max_count                   INTEGER NOT NULL DEFAULT 0 CHECK(max_count >= 0),
    updated_at_ms               INTEGER NOT NULL,
    row_version                 INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE directory_quotas (
    logical_directory           TEXT PRIMARY KEY NOT NULL,
    current_count               INTEGER NOT NULL DEFAULT 0 CHECK(current_count >= 0),
    max_count                   INTEGER NOT NULL DEFAULT 0 CHECK(max_count >= 0),
    enabled                     INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    created_at_ms               INTEGER NOT NULL,
    updated_at_ms               INTEGER NOT NULL,
    row_version                 INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE quota_reservations (
    reservation_id              TEXT PRIMARY KEY NOT NULL,
    file_key                    TEXT NOT NULL UNIQUE,
    logical_directory           TEXT NOT NULL,
    created_at_ms               INTEGER NOT NULL
);

CREATE TABLE applied_quota_events (
    event_id                    TEXT PRIMARY KEY NOT NULL,
    sequence_number             INTEGER NOT NULL UNIQUE,
    applied_at_ms               INTEGER NOT NULL
);
```

Before a write, one quota transaction checks tenant and directory limits,
increments counts, and inserts a reservation. The `ACCEPTED` projection
consumes the reservation without incrementing again. A failure before the
physical write, or a successful physical deletion, removes the reservation
and decrements counts. Startup reconciles each reservation against the journal
and physical file: an established fact consumes it; no fact rolls it back.

`DELETE_SUCCEEDED` and `DEAD_LETTERED` decrement counts only when a file first
leaves the active set. Each quota event is recorded in
`applied_quota_events`, making reservation consumption and decrementing
independently idempotent. Reconciliation recalculates from active metadata
rows, not from physical directory counts; physical scans are only for metadata
loss recovery.

Metadata and quota databases cannot share a SQLite transaction. The projector
commits metadata idempotently, then quota idempotently, and advances the cursor
only after both succeed. Replay fills the missing side through each applied
events table. Reconciliation never advances a cursor for a partially applied
event.

After a valid snapshot covers a sequence and the corresponding journal prefix
has been compacted, maintenance may delete applied-event rows through that
sequence. Cleanup failure affects space only, never correctness.

## 7. queue.log Binary Version 1

There is no global header; every record is independently scannable. Integers
are big-endian.

```text
offset  size  field
0       4     magic = ASCII "STW1"
4       4     frameLength, including every byte in this frame
8       8     sequenceNumber, >= 1
16      4     payloadLength
20      N     UTF-8 canonical JSON payload
20+N    4     CRC32(sequenceNumber bytes + payloadLength bytes + payload)
```

`frameLength = 24 + payloadLength`. Payload is limited to 1 MiB. A frame shorter
than 24, out of bounds, with the wrong magic, or with an invalid CRC is
corrupt. Canonical JSON uses fixed field names, omits null optional values,
uses uppercase enum names, and uses UTC millisecond timestamps.

Scanning may repair only a bad tail: an incomplete or CRC-invalid final frame
is truncated to the last complete frame and the original length is recorded.
Corruption in a complete frame and sequence gaps or regressions are never
skipped; the tenant enters `DOWN`.

## 8. JsonLines Version 1

Each line is one complete `QueueEventRecord` JSON document and ends with LF.
`schemaVersion=1`, `sequenceNumber`, and `eventId` are required. Each line has
`payloadCrc32`, calculated over canonical UTF-8 JSON with that field removed.

Only a missing final LF, incomplete final JSON, or final-line CRC error may be
truncated. Existing non-empty logs keep their format; configuration affects
new or empty logs only.

## 9. Journal State Files

`queue.state.json` version 1:

```json
{
  "schemaVersion": 1,
  "format": "BINARY_V1",
  "baseOffset": 0,
  "tailOffset": 0,
  "lastSequenceNumber": 0,
  "corruptTailDetected": false,
  "lastCorruptTailOffset": null,
  "repairCount": 0,
  "updatedAt": "2026-09-18T00:00:00Z"
}
```

State is an acceleration hint and may never exceed `queue.log`. Missing,
stale, or corrupt state is rebuilt by scanning the log. `baseOffset` is the
logical number of bytes removed by compaction and
`tailOffset = baseOffset + current queue.log length`.

`projector.cursor.json` version 1 stores tenant ID, next offset, last sequence,
last event ID, and update time. The cursor advances only after the SQLite
transaction commits; `applied_events` makes a lagging cursor safe to replay.

## 10. Projection Snapshot

`projection.snapshot.json` version 1 contains tenant ID, through offset,
through sequence, creation time, active files, tenant quota, directory quotas,
and `contentCrc32`. Active files sort by file key and directory quotas by
logical directory for deterministic output.

A snapshot is created only after the projector reaches the current tail. The
file is read and CRC-checked after writing and before compaction. Recovery
accepts only a CRC-valid snapshot whose offset is within
`[baseOffset, tailOffset]` and whose sequence is no greater than the log tail.

## 11. State Transition Table

| Event | Allowed previous state | Result state | Important side effect |
| --- | --- | --- | --- |
| `ACCEPTED` | absent | `PENDING` | Create metadata and consume quota reservation |
| `PROCESSING_STARTED` | `PENDING`/`FAILED` | `PROCESSING` | Store new lease and start time |
| `PROCESSING_FAILED` | `PROCESSING`, same lease | `FAILED` or `PERMANENTLY_FAILED` | Increment retry, clear lease, calculate availability |
| `PROCESSING_TIMED_OUT` | `PROCESSING`, same lease | `PENDING` | Clear lease without incrementing retry |
| `PROCESSING_COMPLETED` | `PROCESSING` or released same lease | `COMPLETED` | Clear lease and store completion time |
| `DELETE_REQUESTED` | `COMPLETED` | `DELETE_REQUESTED` | Queue physical deletion |
| `DELETE_SUCCEEDED` | `DELETE_REQUESTED` | removed after `DELETE_SUCCEEDED` | Decrement quota and remove active row |
| `DEAD_LETTERED` | `PERMANENTLY_FAILED` | removed after `DEAD_LETTERED` | Decrement quota and remove active row |

Replaying an event ID has no side effects. Repeating a valid final operation
with the same lease is idempotent success. A different lease, missing prior
event, or illegal reverse transition is a conflict.

## 12. Write Transaction Boundary

1. Generate a file key and reservation ID.
2. Atomically create the quota reservation and increment tenant/directory
   counts.
3. Write a same-directory temporary file, force according to configuration,
   and atomically move it into place.
4. Append `ACCEPTED`: `DURABLE` waits for force, `BALANCED` waits for write,
   and `ASYNC` waits only for entry into the bounded queue.
5. Update the active cache and enqueue projection work.
6. Idempotently commit `ACCEPTED` to metadata and quota, remove the
   reservation, and advance the cursor only after both sides succeed.

Compensation follows the invariant that a physical file must not coexist with
released quota. A residual file that cannot be deleted is owned by orphan
recovery.

`IdempotentStoragePool.writeIdempotently` stores the caller's operation ID on
the `ACCEPTED` event and `files.import_operation_id`. The tenant-scoped partial
unique index and striped admission return the original file key after retry or
restart without consuming quota or appending another `ACCEPTED` fact.

Watcher imports first reserve a strong source fingerprint in
`source-cleanup.db`, then perform the idempotent write, activate the cleanup
row, and finally record statistics. Capacity rejection happens before the
storage write. A failure after `ACCEPTED` but before activation is retried with
the deterministic operation ID.

## 13. Deletion And Dead-Letter Boundaries

Completion does not delete immediately. The reaper handles only
`DELETE_REQUESTED`; a missing physical file is idempotent success. It appends
`DELETE_SUCCEEDED`, and only projection of that event removes metadata and
quota.

`MOVE_TO_DEAD_LETTER` is an atomic move on the same file system; cross-file
system configuration is rejected at startup. After a successful move,
`DEAD_LETTERED` is appended. If appending fails, dead-letter scanning restores
the event from the file key and never moves the file back to active storage.

## 14. Startup Recovery Order

1. Acquire the exclusive runtime-directory lock; reject startup on failure.
2. Validate configuration and root boundaries.
3. Load tenants and mount and probe volumes.
4. Run `quick_check` on metadata/quota databases and back up damaged files.
5. Scan and repair journal tails and rebuild `queue.state`.
6. Validate snapshots and cursors.
7. Rebuild damaged projections from snapshot plus journal, or continue
   idempotent replay from the cursor.
8. Reconcile quota reservations.
9. Load the active cache in batches and reconcile quota.
10. Run startup orphan recovery when configured.
11. Mark the runtime `RUNNING`, then start periodic tasks and watchers.

By default, an intermediate journal corruption, unknown schema, or sequence
conflict fails fast. With fail-fast disabled, other tenants may run, but the
affected tenant stays isolated and `DOWN` and accepts neither writes nor
claims.

## 15. Crash Recovery Matrix

| Crash point | Deterministic handling after restart |
| --- | --- |
| After reservation, before physical write | Roll back reservation and counts |
| During temporary-file write | Remove residual temporary file and roll back reservation |
| Final file complete, before `ACCEPTED` | Orphan scan creates `ACCEPTED` and consumes reservation |
| `ACCEPTED` complete, before SQLite | Replay creates projection and consumes reservation |
| SQLite commit, before cursor | Replay sees `applied_events` and advances idempotently |
| During snapshot temporary file | Ignore/remove temporary file and use previous snapshot |
| During compaction replacement | Use state, snapshot, and complete-frame scan to retain a safe prefix |
| After `DELETE_REQUESTED`, before deletion | Reaper retries deletion |
| After deletion, before `DELETE_SUCCEEDED` | Missing file is success and event is appended |
| After dead-letter move, before event | Dead-letter scan appends `DEAD_LETTERED` |
| Corrupt `metadata.db` | Back up and rebuild from snapshot plus journal |
| Corrupt `quotas.db` | Back up and rebuild from active metadata rows |

## 16. Compaction Invariants

Compaction is allowed only when the projector cursor equals the tail, a valid
snapshot covers the cursor, processed bytes exceed the threshold, and no
append/read lock is held. Under the tenant journal exclusive lock, write the
unprocessed suffix to a temporary file, force it, atomically replace the log,
and atomically write state. Any failure retains the original log or a complete
suffix recoverable from the snapshot.

## 17. Source Cleanup Database

`source-cleanup.db` is opened lazily and contains `source_cleanup_jobs` with a
unique `(watcher_id, source_path)` key. Each row persists the normalized path,
versioned size/time/content-sample fingerprint, import operation ID, resulting
file key, action and target, failure directory, retry policy and count, state,
due time, timestamps, and claim lease. The due index is ordered by state,
next-attempt, lease, and update time.

`IMPORTING` reservations older than the configured timeout are removed so a
later scan can reserve again and reuse the deterministic storage operation ID.
DELETE and MOVE always recapture the fingerprint; mismatch removes only the
obsolete job and leaves the replacement source untouched. Successful actions
and missing sources remove the row. Exhausted failures move matching content to
`<failureDirectory>/<watcherId>/`; a failed quarantine becomes terminal.

The database, reservation recovery, terminal pruning, claims, and `VACUUM` are
all gated by source-cleanup enabled, global watcher enabled, and at least one
enabled watcher configuration. No gate means no database creation or access.

## 18. Shutdown Invariants

Shutdown first blocks new writes and claims, then stops watchers and cleanup,
drains journal writers and metadata queues, flushes state/cursor, checkpoints
SQLite, and finally releases the directory lock. If the shutdown timeout is
exceeded, return an explicit failure and mark the runtime `FAILED`; an
undrained runtime must never be reported as successfully closed.
