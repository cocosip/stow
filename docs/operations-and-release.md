# Operations And Release Guide

## Runtime Directories

A Stow runtime allows only one process to own a set of runtime directories.
`metadata` stores tenants and SQLite projections, `quota` stores quota ledgers,
`queue` stores per-tenant journals, `watchers` stores watcher configuration and
import history, and volume directories store physical files. Do not share
these directories between processes.

At startup the runtime acquires an exclusive lock, opens tenant and quota
databases, scans journals, repairs only deterministic truncated tails, and
replays projections from the cursor and snapshot. Unknown schema or sequence
values and ambiguous frame corruption fail startup instead of silently losing
data.

## Normal Processing

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
retention settings, `StorageMaintenance` emits delete events; metadata and
quota are released only after the projection confirms `DELETE_SUCCEEDED`.
Report processing errors with `fail(lease, message)`. Once the retry limit is
reached, `permanentlyFailedDisposition` moves the item to dead-letter or
retains it.

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
call `checkDatabases` again and verify file content and quota counts.

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
cannot be identified safely are not deleted. Record every cleanup error and
retry it during the next maintenance cycle.

## Watcher Source Cleanup

Watcher DELETE and MOVE actions use `{watcherDirectory}/source-cleanup.db` so
an imported source is not forgotten when the process exits between storage and
the post-import action. The worker is intentionally dormant unless source
cleanup, the global watcher option, and at least one watcher configuration are
all enabled. When dormant it does not create or open the database and does not
run reservation recovery, pruning, claims, or optimization.

When `maxActiveJobs` is reached, scans report a deferred import and leave the
source untouched. Investigate terminal failures and the configured failure
directory before increasing capacity. Do not delete rows manually: stale
IMPORTING reservations, terminal retention, and `VACUUM` are bounded worker
operations. Preserve `source-cleanup.db` with the watcher directory during an
incident or backup.

Shutdown stops new watcher scans, waits for admitted scans, then stops cleanup
claims and waits for active source actions before journal, projection, and
volume resources close.

## Build And Version Management

The Maven reactor uses one project version and centrally managed dependency and
plugin versions. The root POM uses `${revision}` with `0.1.0-SNAPSHOT` as the
development default. Child modules do not declare independent versions and
reactor dependencies use `${project.version}`.

Published modules use `flatten-maven-plugin` in
`resolveCiFriendliesOnly` mode. Source POMs retain `${revision}` while install
and deploy use a flattened POM with a concrete version. Samples and benchmarks
set `maven.deploy.skip=true`; only `stow-core` and
`stow-spring-boot-starter` are release artifacts.

The build-baseline test checks recursive module discovery, version alignment,
dependency/plugin management, flattened consumer POMs, and deploy skip flags.

## GitHub Actions And Central Portal

Pushes and pull requests targeting `master` run `./mvnw -B -ntp verify` on
Linux and Windows with JDK 21. The branch workflow never deploys artifacts.

Tags matching `v*` run the same verification and then execute:

```bash
./mvnw -B -ntp -Drevision="$RELEASE_VERSION" -Prelease deploy
```

The release profile signs artifacts and uploads the bundle to Sonatype Central
Portal. Configure these repository secrets before creating a tag:

| Secret | Purpose |
| --- | --- |
| `MAVEN_CENTRAL_USERNAME` | Central Portal user-token username |
| `MAVEN_CENTRAL_TOKEN` | Central Portal user-token password |
| `MAVEN_GPG_PRIVATE_KEY` | ASCII-armored private signing key |
| `MAVEN_GPG_PASSPHRASE` | Signing key passphrase |

The Maven server id is `central`. No credentials are committed to source
control. A tag such as `v1.0.0` publishes version `1.0.0`; the leading `v` is
removed before passing `revision` to Maven.

## Incident Order

1. Stop writers, preserve the original directories and logs, and do not edit
   SQLite or `queue.log` directly.
2. Copy `metadata`, `quota`, `queue`, and volume directories as read-only
   evidence.
3. Check `runtime.health()` and `maintenance.checkDatabases()` to distinguish
   journal, projection, database, and volume failures.
4. Replay projections first; rebuild metadata or quota only after confirming
   database corruption.
5. Reclaim timed-out processing and recover physical orphans, then reconcile
   quotas before resuming writes.
6. Record the time, tenant, command, and returned statistics as incident or
   release evidence.

Never delete `queue.log`, cursor, snapshot, or reservation files manually to
clear a backlog. Recovery code and the public maintenance APIs own those files.
