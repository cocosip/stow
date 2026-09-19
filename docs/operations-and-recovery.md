# Operations And Recovery

## Runtime Directories

A Stow runtime allows only one process to own a set of runtime directories.
`metadata` stores tenants and SQLite projections, `quota` stores quota ledgers,
`queue` stores per-tenant journals, `watchers` stores watcher configuration and
import history, and volume directories store physical files. Do not share these
directories between processes.

At startup the runtime acquires an exclusive lock, opens tenant and quota
databases, scans journals, repairs only deterministic truncated tails, and
replays projections from the cursor and snapshot. Unknown schema or sequence
values and ambiguous frame corruption fail startup instead of silently losing
data.

## Normal Processing

All operations go through the public services exposed by `StowRuntime`:

```java
try (StowRuntime runtime = Stow.open(configuration)) {
    TenantContext tenant = runtime.tenantManager().get("tenant-a");
    StoragePool pool = runtime.storagePool();
    String fileKey = pool.write(tenant, ContentSources.of(path), WriteOptions.defaults());
    ClaimedFile claimed = pool.claimNext(tenant).orElseThrow();
    pool.complete(claimed.lease());
}
```

`complete` does not immediately delete the physical file. According to the
retention settings, `StorageMaintenance.cleanupCompleted` emits delete events;
metadata and quota are released only after the projection confirms
`DELETE_SUCCEEDED`. Report processing errors with `fail(lease, message)`. Once
the retry limit is reached, `permanentlyFailedDisposition` moves the item to
dead-letter or retains it.

## Diagnosis, Replay, And Rebuild

When a queue is behind, inspect `projectionMaintenance().state(tenantId)` and
compare the cursor with the journal tail before calling `replay(tenantId)`.
Create snapshots and compact only after the projector catches up:

```java
QueueProjectionMaintenance projection = runtime.projectionMaintenance();
projection.replay("tenant-a");
projection.snapshot("tenant-a");

StorageMaintenance maintenance = runtime.maintenance();
DatabaseHealthReport health = maintenance.checkDatabases();
maintenance.rebuildMetadata("tenant-a");
maintenance.rebuildQuota("tenant-a");
maintenance.optimizeDatabases();
```

Before rebuilding, preserve database backups and verify that journals and
volumes are readable. Metadata rebuilds use the snapshot and journal as their
source of truth; quota rebuilds use active metadata rows. After rebuilding,
call `checkDatabases` again and verify `storagePool().findFileInfo`, file
content, and quota counts.

## Cleanup And Orphans

```java
maintenance.reclaimTimedOutProcessing(configuration.cleanup().processingTimeout());
maintenance.recoverAllOrphans();
maintenance.reconcileAllQuotas();
maintenance.cleanupJunkFiles();
maintenance.cleanupEmptyDirectories();
```

Orphan recovery adopts only physical files that match the Stow file-key naming
rules but have no corresponding `ACCEPTED` event or metadata row. Files that
cannot be identified safely are not deleted. Record every cleanup error summary
and retry it during the next maintenance cycle.

## Incident Order

1. Stop writers, preserve the original directories and logs, and do not edit
   SQLite or `queue.log` directly.
2. Copy `metadata`, `quota`, `queue`, and volume directories as read-only
   evidence.
3. Check `runtime.health()` and `maintenance.checkDatabases()` to distinguish a
   journal, projection, database, or volume failure.
4. Replay projections first; rebuild metadata or quota only after confirming
   database corruption.
5. Reclaim timed-out processing and recover physical orphans, then reconcile
   quotas before resuming writes.
6. Record the time, tenant, command, and returned statistics as release or
   incident evidence.

Never delete `queue.log`, cursor, snapshot, or reservation files manually to
clear a backlog. Recovery code and the public maintenance APIs own those files.
