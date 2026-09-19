# Configuration Reference

`StowConfiguration.builder()` is the framework-neutral configuration entry
point. Paths are normalized to absolute paths during `build()`, and numbers,
durations, identifiers, and collections are validated at the same boundary.
The Spring Boot starter binds the same model with kebab-case `stow.*` properties.

## Paths And Tenants

| Group | Java builder / Spring property | Default |
| --- | --- | --- |
| paths | `metadataDirectory` / `metadata-directory` | `./stow-metadata` |
| paths | `quotaDirectory` / `quota-directory` | `./stow-quota` |
| paths | `queueDirectory` / `queue-directory` | `./stow-queue` |
| paths | `watcherDirectory` / `watcher-directory` | `./stow-watchers` |
| tenant | `autoCreateTenants` / `auto-create-tenants` | `false` |
| tenant | `defaultQuota` / `default-quota` | `0` (unlimited) |
| tenant | `preconfiguredTenants` / `preconfigured-tenants` | `[]` |

Configure storage volumes with `volumes`. Each `id` must be unique, `mountPath`
must be a safe directory, `shardingDepth` must be `0..3`, `bufferSize` must be
positive, and `forceFlushAfterWrite` defaults to `false`.

## Metadata, Storage, And SQLite

| Group | Property | Default |
| --- | --- | --- |
| metadata | `background-persistence` | `true` |
| metadata | `max-queue-size` | `100000` |
| metadata | `drain-batch-size` | `2000` |
| metadata | `soft-merge-threshold-percent` | `90` |
| metadata | `startup-load-batch-size` | `2000` |
| metadata | `shutdown-drain-timeout` | `30s` |
| metadata | `persistence-interval` | `2s` |
| storage | `completion-guard-stripes` | `256` |
| storage | `empty-queue-reclaim-batch-size` | `32` |
| storage | `background-reclaim-batch-size` | `8` |
| storage | `reclaim-cooldown` | `30s` |
| storage | `background-reclaim-enabled` | `true` |
| sqlite | `journal-mode` | `WAL` |
| sqlite | `synchronous-mode` | `NORMAL` |
| sqlite | `cache-size-kb` | `-4000` |
| sqlite | `busy-timeout` | `5s` |
| sqlite | `checkpoint-after-batch` | `false` |

## Retry, Journal, And Projection

| Group | Property | Default |
| --- | --- | --- |
| retry | `max-retry-count` | `3` |
| retry | `initial-delay` | `5s` |
| retry | `exponential-backoff` | `true` |
| retry | `max-delay` | `5m` |
| journal | `enabled` | `true` (cannot be disabled) |
| journal | `projection-enabled` | `true` |
| journal | `format` | `BINARY_V1` |
| journal | `ack-mode` | `DURABLE` |
| journal | `state-flush-debounce` | `1s` |
| journal | `linger` | `1ms` |
| journal | `max-batch-records` | `16` |
| journal | `max-batch-bytes` | `262144` |
| journal | `writer-idle-timeout` | `30s` |
| journal | `async-queue-capacity-per-tenant` | `8192` |
| journal | `balanced-flush-window` | `5ms` |
| projection | `max-records-per-tenant-cycle` | `64` |
| projection | `max-tenants-per-cycle` | `8` |
| projection | `busy-cycle-delay` | `500ms` |
| projection | `idle-cycle-delay` | `5s` |
| projection | `cycle-time-budget` | `2s` |

Supported journal formats are `BINARY_V1` and `JSON_LINES_V1`; ACK modes are
`DURABLE`, `BALANCED`, and `ASYNC`. Do not disable the journal in production.

## Snapshots, Compaction, Cleanup, And Orphan Recovery

| Group | Property | Default |
| --- | --- | --- |
| snapshot | `enabled` | `true` |
| snapshot | `interval` | `15m` |
| snapshot | `minimum-progress-bytes` | `1048576` |
| compaction | `enabled` | `true` |
| compaction | `minimum-processed-bytes` | `4194304` |
| cleanup | `enabled` | `true` |
| cleanup | `interval` | `1h` |
| cleanup | `initial-delay` | `1m` |
| cleanup | `processing-timeout` | `30m` |
| cleanup | `completed-retention` | `0s` |
| cleanup | `failed-retention` | `3d` |
| cleanup | `permanently-failed-disposition` | `MOVE_TO_DEAD_LETTER` |
| cleanup | `batch-size-per-tenant` | `500` |
| orphan-recovery | `enabled` | `false` |
| orphan-recovery | `run-on-startup` | `false` |
| orphan-recovery | `interval` | `6h` |

## Statistics, Watchers, And Validation

| Group | Property | Default |
| --- | --- | --- |
| statistics | `enabled` | `false` |
| statistics | `window-size` | `5m` |
| statistics | `retention` | `1h` |
| statistics | `max-series` | `16384` |

`watchers` is empty by default. Required watcher fields are `watcher-id`,
`tenant-id`, and `watch-path`. Unless specified, the defaults are
`enabled=true`, `recursive=false`, `globs=["*"]`, `post-import-action=KEEP`,
`poll-interval=5s`, `minimum-file-age=0s`, `stability-check-interval=100ms`,
`stability-check-count=2`, `concurrent-imports=1`, `history-retention=7d`, and
`history-flush-interval=1s`.

See the [API contract](stow-api-contract.md) for complete types and exception
semantics.
