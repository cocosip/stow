# Locus Alignment Review Register

## 1. Purpose

This document is the complete, item-by-item register of the stow ↔ Locus
alignment code review: every finding, its current status, and — for items
not fixed — whether the difference is an intentional stow contract decision
or remaining work. It complements the design baseline in `stow-design.md`
§25 and the contracts in `stow-api-contract.md` and
`stow-persistence-contract.md`.

## 2. Review Baseline And Method

- Reference: Locus 2.0.2 (the local Locus working tree matches the `v2.0.2`
  tag for `src/` and `tests/`; the original design baseline was Locus 2.0.0,
  commit `292bd2c`).
- Method: six domain audits (journal, scheduler/leases, projection/SQLite,
  quota, cleanup/watcher, write path/runtime) comparing behavior, defaults,
  and failure paths against the Locus implementation, with stow's own
  contracts as the tie-breaking authority. Every P0 finding was re-verified
  against source before being fixed.
- Stow does not promise disk-format or configuration interoperability with
  Locus (`stow-persistence-contract.md` §1). Behavioral alignment is the
  goal, not byte compatibility.

## 3. Summary

95 findings were recorded: 10 P0, 45 P1, 40 P2.

| Status | Count | Meaning |
| --- | --- | --- |
| FIXED | 75 | Implemented and covered by tests; `mvn clean verify` green |
| INTENTIONAL | 20 | stow's contract deliberately chooses stricter or different behavior; do not change without a contract revision (§5) |
| OPEN | 0 | Nothing queued: every finding is resolved or an intentional deviation |

All 10 P0 and all 45 P1 findings are resolved (9 former P1s are
INTENTIONAL: J4, J5, S5, Q6, Q7, C10, W7, W10, W11). The register is
closed.

Status legend used below: **FIXED**, **PARTIAL**, **INTENTIONAL** (§5),
**OPEN** (§6). Priority is the original audit priority (P0 blocker, P1
semantic/capability, P2 minor).

## 4. Findings Register

### 4.1 Journal and queue events (13 findings)

| # | P | Finding | Status |
|---|---|---------|--------|
| J1 | P0 | BALANCED ack never fsynced; `balancedFlushWindow` was dead config, so acknowledged events could be lost across the whole process lifetime | FIXED — forces immediately with no backlog and at least once per window; knob is live |
| J2 | P1 | Read-batch corruption throws instead of returning the valid prefix with corrupt-tail normalization (Locus `ReadBatchAsync` + `NormalizeReadOffset`); `JournalScanner` repair decisions sniff exception message text | FIXED — read batches return the valid prefix with a corrupt-tail flag, repair decisions key on typed corruption reasons (structure/CRC/sequence/schema), an empty prefix at a corrupt frame triggers tail repair, and misaligned offsets normalize to the nearest verified frame; mid-file corruption still latches DOWN |
| J3 | P1 | Tenant journal writer was permanently dead after the first write failure | FIXED — a successful batch clears the stored failure; only a dead worker thread rejects appends |
| J4 | P1 | Sequence gaps latch the tenant DOWN instead of Locus's detect → metrics → orphan-recovery self-heal | INTENTIONAL (persistence §7 documents DOWN) — revisit only with a contract revision |
| J5 | P1 | Mid-file corruption is not truncated-and-continued; final-frame repair sniffs message text | INTENTIONAL for the strictness (see J2 for the repair-decision refactor) |
| J6 | P1 | `linger`, `maxBatchBytes`, `stateFlushDebounce` accepted but inert | FIXED — linger coalesces, batches bound by records and bytes (encode-during-collect), state persistence debounced |
| J7 | P2 | `readBatch` echoes an out-of-range offset instead of clamping to the tail | INTENTIONAL — `JournalReadBatch`'s monotonic-offset invariant protects every replay loop |
| J8 | P2 | `compact` rejected out-of-range offsets and returned void | FIXED — offsets clamped to `[base, tail]` (return type remains void; Locus returns a cursor) |
| J9 | P2 | `QueueEventRecord` payload schema differs (frame-level CRC, `leaseId` added, non-null invariants, 1 MiB frame cap, ms timestamps) | INTENTIONAL (api-contract §4, persistence §7) |
| J10 | P2 | Event timestamps truncated to milliseconds | INTENTIONAL (persistence §7) |
| J11 | P2 | Tenant-ID validation much stricter than Locus | INTENTIONAL (api-contract §3) |
| J12 | P2 | `tenantIds()` returns an in-memory snapshot, not a directory enumeration | FIXED — tenant IDs enumerate the queue directory (Locus `GetTenantIdsAsync`) unioned with the in-memory snapshot |
| J13 | P2 | No journal write-path diagnostics/metrics (append batches, flushes, corrupt tails, gaps) | FIXED — journal write-path statistics (append batches, single/multi split, records, bytes, durations, corrupt tails detected/repaired) exposed via `FileQueueEventJournal.writePathStatistics()` |

### 4.2 Scheduler, claims, leases, retry (14 findings)

| # | P | Finding | Status |
|---|---|---------|--------|
| S1 | P0 | A late `complete()` after timeout reclaim (or after a same-lease `fail()`) threw; Locus lets the completion win | FIXED — reclaimed-lease completions converge the row; complete-vs-fail keeps one deterministic winner with the loser throwing |
| S2 | P0 | Empty claims never reclaimed stuck leases; reclaim knobs were dead config | FIXED — inline reclaim on empty claim plus cooldown-gated background reclaim; all four knobs live |
| S3 | P1 | `Duration.ZERO` timeout reclaimed every in-flight lease | FIXED — zero/negative timeouts are no-ops |
| S4 | P1 | Claim ordering by `created_at` instead of ready-time FIFO with delayed-queue promotion | FIXED — claim candidates order by ready time (`COALESCE(available_at_ms, created_at_ms)`), so a retried file competes in FIFO by the moment it became ready; future-dated rows stay excluded until their availability passes |
| S5 | P1 | Resting status after retryable failure is `FAILED` instead of `Pending` + future availability | INTENTIONAL (api-contract §169 defines the stow enum) |
| S6 | P1 | `complete()` did not append `DELETE_REQUESTED`; deletion waited for the cleanup cycle | FIXED — both events append atomically, matching Locus and design §13 |
| S7 | P1 | No per-tenant recovery gate; a duplicate `PROCESSING_TIMED_OUT` for one lease wedges the tenant projection | FIXED — per-tenant single-flight reclaim plus the Locus stale-timed-out skip (a duplicate or superseded recovery event converges silently) |
| S8 | P2 | `complete()` on missing metadata threw where Locus returns silently | INTENTIONAL — per-tenant stores cannot distinguish a foreign lease from a reaped row |
| S9 | P2 | Completed rows kept stale `last_error` / `last_failed_at` / `available_at` | FIXED — completion projection clears them |
| S10 | P2 | `PERMANENTLY_FAILED` rows reported a fake availability time | FIXED — permanent failures persist null availability |
| S11 | P2 | read/info/location/status skipped tenant validation; unknown tenants reported as disabled | FIXED — `TenantNotFoundException` vs `TenantDisabledException` distinguished |
| S12 | P2 | `FileAlreadyProcessingException` never thrown; no mark-processing/reset capability | INTENTIONAL (api-contract: claim-based model) — stow's atomic claim-with-lease supersedes Locus's separate mark-processing/reset capability; `FileAlreadyProcessingException` remains for providers |
| S13 | P2 | Claim-rollback failures silently ignored | FIXED — logged at WARN (a CAS loss leaves a phantom PROCESSING row for the reaper) |
| S14 | P2 | `completionGuardStripes` accepted but hardcoded 256 | FIXED — threaded into the pool's striped locks |

### 4.3 Projection, SQLite metadata, database recovery (18 findings)

| # | P | Finding | Status |
|---|---|---------|--------|
| P1 | P0 | Background projection starved tenants beyond `maxTenantsPerCycle` (no rotation) | FIXED — rotating start index; every tenant is served |
| P2 | P0 | No startup database health check, auto-recovery, or orphan-tenant pipeline | FIXED — startup `quick_check`, damaged metadata rebuilt (backup + snapshot/journal replay) then quota recomputed |
| P3 | P1 | Automatic snapshots and compaction never triggered; `compact()` unreachable from the API | FIXED — wired into the projection cycle per `snapshot.*`/`compaction.*` |
| P4 | P1 | `rebuildMetadata`/`rebuildQuota` have no exclusive rebuild lock and move the live database | FIXED — rebuilds run under the tenant's exclusive metadata and quota stripe locks, draining in-flight operations before the database files move (Locus `BeginDatabaseRebuildAsync`) |
| P5 | P1 | Rebuild did not reconcile quota counts afterwards | FIXED — manual rebuild and startup recovery recompute from the restored active set |
| P6 | P1 | `rebuildFromMetadata` wiped explicit directory limits and in-flight reservations | FIXED — limits preserved, reservations kept and counted, empty unlimited rows pruned |
| P7 | P1 | Snapshot content lacked creation time, active files, and quota state (contract §10) | FIXED — snapshot carries `createdAt`, active files, tenant/directory quota state, `contentCrc32` |
| P8 | P2 | `state()` reported `Instant.now()` as the snapshot time | FIXED — reports the persisted creation time |
| P9 | P2 | Manual snapshot could persist a mid-log snapshot | FIXED — the projector is caught up before the bounds check (Locus-aligned soft gate) |
| P10 | P2 | Cursor persistence has no debounce (write amplification vs Locus 1 s dirty-cache) | INTENTIONAL (persistence §9) — the cursor persists after every committed projection batch; stow is more durable by design and the write is one small atomic JSON file per batch |
| P11 | P2 | Health check maps SQLITE_BUSY/LOCKED to DOWN; no startup retries (`integrity_check(1)` vs `quick_check`) | FIXED — busy/locked failures are retried briefly and reported DEGRADED instead of DOWN; `quick_check` itself remains INTENTIONAL (contract §14) |
| P12 | P2 | `optimizeDatabases` uncoordinated and reported zero released bytes | FIXED — `optimizeDatabases` runs under each tenant's exclusive metadata and quota stripes (VACUUM takes the SQLite write lock); released bytes were measured in the previous batch |
| P13 | P2 | `checkpointAfterBatch` accepted but inert | FIXED — `PRAGMA wal_checkpoint(PASSIVE)` after metadata batch commit when enabled |
| P14 | P2 | `applied_events` / `applied_quota_events` never pruned | FIXED — pruned up to the compacted sequence after compaction (contract §6) |
| P15 | P2 | `ActiveFileCache` is vestigial (never read by the pool) | FIXED — `ActiveFileCache` removed; every read path queries SQLite directly |
| P16 | P2 | Reducer throws on conflicts where Locus skips stale events and synthesizes missing rows | INTENTIONAL (persistence §11 strictness) |
| P17 | P2 | `PROCESSING_FAILED` availability handled; retry-count merge prefers `max(current+1, event)` vs Locus event-first | FIXED — the retry-count merge is event-first (Locus `record.RetryCount ?? existing`) |
| P18 | P2 | `replay()` hardcodes a 256-record batch | FIXED — the manual replay batch size is configurable (`projection.manualReplayBatchSize`, default 256) |

### 4.4 Tenant and directory quotas (11 findings)

| # | P | Finding | Status |
|---|---|---------|--------|
| Q1 | P0 | No startup reconciliation of quota reservations; leaked reservations permanently consumed quota | FIXED — startup reconciles against the projected active set, covering journal tenants and quota-only tenants |
| Q2 | P1 | A failed atomic move after publish leaked the reservation in the live process | FIXED — every no-candidate write failure rolls the reservation back |
| Q3 | P1 | `consume` hard-failed on a missing reservation and wedged the projection pipeline | FIXED — consumption marks the ledger and tolerates missing reservations (Locus-aligned) |
| Q4 | P1 | Orphan recovery enforced quota limits, stranding physical orphans at full quota | FIXED — `forceReserve` bypasses limit checks for recovery |
| Q5 | P1 | Release decremented the event's directory, corrupting counts when the charged directory differed | FIXED — release resolves the charged directory from the surviving reservation row |
| Q6 | P1 | Journal-append failure keeps the published file + reservation where Locus deletes and rolls back | INTENTIONAL (persistence §12: residual files are owned by orphan recovery) |
| Q7 | P1 | No global limit API; `0` means unlimited without per-tenant fall-back | INTENTIONAL (api-contract documents the reduction) |
| Q8 | P2 | Directory normalization rejects inputs Locus canonicalizes | INTENTIONAL (stow validation contract) |
| Q9 | P2 | Quota exceptions carried only a message | FIXED — structured tenant/directory, current, and max fields |
| Q10 | P2 | Read APIs create rows as a side effect (`ensureTenant`/`ensureDirectory` on reads) | FIXED — quota reads no longer create rows: the singleton and directory rows are only ensured on writes, and reads of absent rows return zero counts with no explicit limit |
| Q11 | P2 | Metadata rows whose physical file vanished keep their quota charge forever | FIXED — scheduled orphaned-metadata cleanup removes such rows and releases their charges via reservation reconciliation (see C6) |

### 4.5 Cleanup, orphan recovery, watcher (20 findings)

| # | P | Finding | Status |
|---|---|---------|--------|
| C1 | P0 | Permanent-failure reaper never converged when the physical file was already missing | FIXED — missing source converges to `DEAD_LETTERED` recording the last known path |
| C2 | P1 | `DEAD_LETTERED` event recorded the pre-move path, making dead-lettered files untraceable | FIXED — event carries the actual dead-letter (or last known) path |
| C3 | P1 | `DELETE_REQUESTED` rows were physically deleted regardless of the retention cutoff | FIXED — physical deletion honors the completion-anchored retention window |
| C4 | P1 | Empty-directory sweep deleted tenant roots and shard directories | FIXED — roots and `depth <= shardingDepth` protected; scan depth capped at 20 |
| C5 | P1 | Backup cleanup, empty-directory cleanup, and `optimizeDatabases` were API-only | FIXED — scheduled in the cleanup cycle (daily VACUUM throttle); write-path temp sweeping intentionally stays startup-only (in-flight writes own those files) |
| C6 | P1 | No orphaned-metadata removal (metadata without physical file) — Locus `CleanupOrphanedMetadataAsync` | FIXED — scheduled cleanup removes rows whose physical file vanished (skipping unavailable/unhealthy volumes, repairing corrected canonical paths in place) and releases their quota |
| C7 | P1 | Watchers imported for disabled tenants | FIXED — disabled tenants are skipped, not failed |
| C8 | P1 | Auto-managed watchers: zero stability checks, per-tenant watcher topology | FIXED — one multi-tenant watcher per root with root-level `minFileAge` (5 s default), stability double-probe (100 ms interval), and concurrent imports threaded from the root config |
| C9 | P1 | SUBDIRECTORY_TENANTS mode mints tenant records from directory names | FIXED (semantics decision: Locus-aligned) — `SUBDIRECTORY_TENANTS` never mints tenants from directory names; `autoCreateTenantDirectories` only provisions import directories for existing tenants; files outside a tenant directory are skipped |
| C10 | P1 | `MOVE` post-import layout mirrors the source subtree with `name.N.ext` collisions; Locus flattens with hash suffixes | INTENTIONAL (api-contract: stow layout) — the mirrored subtree with deterministic `name.N.ext` collision suffixes is stow's documented, fingerprint-guarded behavior; Locus's flat hash layout is not interoperable anyway (persistence §1) |
| C11 | P2 | Reapers processed one batch per status per cycle | FIXED — drain-to-empty per run, each fileKey touched at most once |
| C12 | P2 | Reapers ignored volume health | FIXED — both reapers skip files whose volume is missing or unhealthy, so a temporary mount outage cannot produce a false DELETE_SUCCEEDED/DEAD_LETTERED convergence |
| C13 | P2 | Dead-letter layout/options not configurable; no `PurgeMetadataOnly` disposition | FIXED — `PURGE_METADATA_ONLY` disposition converges permanent-failure rows to DEAD_LETTERED and releases quota without touching the physical file (Locus PurgeMetadataOnly); it also does not require a healthy volume |
| C14 | P2 | Orphan recovery fidelity: name pattern gating, `/` logical directory, no tenant-status gating | FIXED — orphan recovery only adopts files for existing, enabled tenants (tenant-status gate threaded from the runtime); name-pattern gating and the `/` logical directory were already in place |
| C15 | P2 | Import pre-checks missing (0-byte skip, exclusive-open skip, unstable = skipped) | FIXED — imports skip 0-byte sources and treat an unstable file between stability probes as a skip for the next scan rather than a failure |
| C16 | P2 | Legacy watcher path lacks post-import retry caps, backoff, quarantine | FIXED — the legacy import path now has the same protections as the durable one: failed post-import actions back off exponentially up to the configured maximum, stop after `maxPostImportActionAttempts`, and quarantine the source into `<failureDirectory>/<watcherId>/`; a failed quarantine move still marks the source terminal |
| C17 | P2 | Watcher registration validation gaps (overlap, tenant existence) | FIXED — watcher registration validates that a single-tenant watcher's tenant exists and that no existing watcher already watches the same directory |
| C18 | P2 | Options defaults (`maxParallelScans` 1 vs 4), no interval clamp, `historyFlushDebounce` dead knob | FIXED — `maxParallelScans` defaults to 4, the scan interval clamps to the Locus bounds [5 s, 1 h], and the global `historyFlushDebounce` knob is live as the history-prune throttle |
| C19 | P2 | Retired-volume handling dead code; no `PurgeMetadataOnly` disposition | FIXED — retired volumes use the Locus disposition model (`KEEP` / purge-metadata-only): the rewired cleaner removes projected rows pointing at retired volumes and releases quota without touching physical storage, exposed via `StorageMaintenance.cleanupRetiredVolumes` |
| C20 | P2 | Fingerprint composition differs (head/tail 64 KB samples + path vs three 4 KB samples) | INTENTIONAL — internally consistent; no interop promised |

### 4.6 Storage pool write path, volumes, tenants, runtime (19 findings)

| # | P | Finding | Status |
|---|---|---------|--------|
| W1 | P0 | Volume selection pinned writes to the fullest volume; power-of-two selector was dead code | FIXED — `PowerOfTwoVolumeSelector.ordered` wired into candidate selection |
| W2 | P0 | Disabled/unknown tenants not enforced on read/info/location/status; missing tenants misclassified | FIXED — full `requireEnabled` enforcement with distinct exceptions |
| W3 | P0 | Filtered statistics queries always returned zeros (OPERATION-only dimension set) | FIXED — `VOLUME` and `WATCHER` dimensions retained, matching Locus defaults |
| W4 | P1 | fileKey generation lacks Locus's burst-shard locality guarantee | FIXED — per-process seeded shard prefix (murmur3 finalizer) overwrites the first two bytes, so bursts of up to 32 keys share one shard directory |
| W5 | P1 | Write retries swallow any `RuntimeException` per candidate instead of IO-only seekable retries | FIXED — only I/O-class failures advance to the next candidate (Locus `IsRetryableWriteFailure`), and a failed write forces a health re-probe of that volume |
| W6 | P1 | Move-failure after publish leaked the reservation | FIXED — rollback runs on every no-candidate failure; append-failure handling is INTENTIONAL (see Q6) |
| W7 | P1 | Durable-write sequence differs (stow temp → fsync → move vs Locus in-place create) | INTENTIONAL — stow is strictly stronger (design §) |
| W8 | P1 | Volume health: no write probe, 250 ms TTL, no mount-time gating | FIXED — 30 s cached probe with a write probe, forced re-probe after write failures and at mount time, and a startup mount gate requiring two consecutive forced passes |
| W9 | P1 | Tenant lifecycle: idempotent create, no storage-path provisioning, no status cache, no `Suspended` | FIXED — idempotent create was already in place; the tenant document is now cached across reads, and creation provisions the tenant's directories under the metadata, quota, queue, and volume roots (Locus storage-path provisioning); `Suspended` remains INTENTIONAL |
| W10 | P1 | Tenant-ID charset stricter than Locus | INTENTIONAL (see J11) |
| W11 | P1 | Extension handling drops extensions Locus preserves verbatim | INTENTIONAL (design §) |
| W12 | P1 | Statistics capability gaps: measurement names, output service, `MaxSeries` bounds | FIXED — the operation dimension now carries Locus measurement names (`storage.write.success.count`, `storage.file.read.count`, `storage.file.dequeued.count`, `storage.file.completed.count`, `watcher.files.imported`, `watcher.scan.count`, `watcher.files.discovered/skipped/failed`); `maxSeries` validates against the Locus bounds [1024, 262144]; an optional periodic logging output service is available (off by default) |
| W13 | P1 | Builder defaults differ (`autoCreateTenants`, `forceFlushAfterWrite`, required volume set, per-tenant preconfiguration) | FIXED — documented decision per default: `forceFlushAfterWrite` now defaults to true (Locus-aligned, strictly safer); `autoCreateTenants=false` and the required non-empty volume set stay as documented stow strictness (see §5); per-tenant preconfiguration exists via `preconfiguredTenants` |
| W14 | P2 | Idempotent-write details (`operationId` cap, per-call lookup vs full index) | INTENTIONAL (api-contract §12) — the operation-ID cap and the striped-admission plus unique-index lookup are contract-documented equivalents of Locus's in-memory index |
| W15 | P2 | `complete()` emitted no `DELETE_REQUESTED` | FIXED (see S6) |
| W16 | P1 | No read-path physical-path self-heal (`TryCorrectMetadataPhysicalPathAsync`) | FIXED — a missing-file read rebuilds the canonical volume path, reads from it when the file is there, and persists the correction (CAS on row version); the orphaned-metadata cleaner reuses the same correction |
| W17 | P2 | Capacity reporting granularity (1 s cache, distinct insufficient-storage messages) | FIXED — insufficient-storage failures distinguish no healthy volumes, volumes that cannot hold the known byte length, and all volumes full (Locus messages); capacity reporting rides the volume probe cache |
| W18 | P2 | Health model missing `journal`/`sqlite` components; no write-path diagnostics/metrics | FIXED — the health model now exposes `journal` (tenant-level state, DOWN when a tenant is latched DOWN) and `sqlite` (quick-check aggregation) components alongside the per-volume entries |
| W19 | P2 | Startup ordering: tenant initialization before journal replay; no readiness gates | FIXED — startup follows the persistence-contract order (lock, tenants, volumes with the mount gate, quick_check, journal scan/repair, replay, quota reconciliation) before RUNNING, and RuntimeQuotaOperationAdmission gates writes on the runtime state |

## 5. Intentional Deviations (do not change without a contract revision)

Grouped from the register above: J4, J5, J7, J9, J10, J11, S5, S8, S12,
P10, P16 (quick_check aspect of P11), Q6, Q7, Q8, C10, C20, W7, W10,
W11, W14, the `Suspended`-status aspect of W9, and the
`autoCreateTenants=false` plus required-volume-set aspects of W13. The
rationale for each is stated in its register row and in
`stow-design.md`, `stow-api-contract.md`, and
`stow-persistence-contract.md`.

## 6. Remaining Work (backlog)

Open P1 items, ordered by expected production impact:

## 6. Remaining Work (backlog)

None. Every one of the 95 findings is either FIXED or an INTENTIONAL
deviation enumerated in §5. Future behavior changes to the intentional
deviations require a contract revision.

## 7. Verification

- `mvn clean verify`: BUILD SUCCESS — 285 core tests (5 symlink-assumption
  skips on Windows), 16 starter tests, 2 integration tests, spotless and
  spotbugs gates, zero compiler warnings under `-Xlint:all`.
- Second-batch regression tests cover: corrupt-tail prefix reads with tail
  repair (`FileQueueEventJournalTest`), stale duplicate timed-out skips and
  ready-time claim ordering (`QueueEventReducerTest`), orphaned-metadata
  removal with quota release (`OrphanedMetadataCleanerTest`), read-path
  physical-path self-heal (`PhysicalPathSelfHealTest`), I/O-only write
  retries (`WriteRetryConditionTest`), and fileKey shard locality
  (`FileKeyGeneratorTest`).
- Third-batch coverage: one multi-tenant watcher per root
  (`FileWatcherAutoManagerTest`), Locus measurement names
  (`WindowedStatisticsRecorderTest`).
- Fourth batch: no dedicated test class; covered by the existing watcher,
  cleanup, journal, and runtime suites (`FileWatcherManagerTest`,
  `WatcherScannerTest`, `StorageMaintenanceTest`,
  `FileQueueEventJournalTest`, `DefaultStowRuntimeTest`).
- Fifth batch: retired-volume purge semantics (`RetiredVolumeCleanerTest`).
- Sixth batch: legacy-path retry cap, backoff, and quarantine
  (`WatcherScannerTest.legacyActionRetriesWithBackoffThenQuarantines`).
- Earlier regression tests cover: late-completion recovery, empty-claim
  inline reclaim, statistics dimension retention, quota reservation
  reconciliation, `forceReserve`, missing-permanent-failure convergence,
  damaged-database startup recovery, and snapshot state round-trips.
- Platform notes: five skipped tests require symbolic-link support and abort
  via JUnit assumptions on Windows environments without it.
