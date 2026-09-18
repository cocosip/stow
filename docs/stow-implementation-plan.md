# Stow 1.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 OpenJDK 21 上一次性实现与 Locus 2.0 功能语义对齐的 Stow 文件存储池及 Spring Boot 适配。

**Architecture:** `stow-core` 使用框架无关的构造器注入和内部组合根，物理文件、每租户 journal、SQLite 投影分别承担多租户内容事实、队列事实和查询投影。`stow-spring-boot-starter` 只绑定配置和生命周期，不复制业务逻辑。

**Tech Stack:** Java 21、Maven、SQLite JDBC、Jackson 2.x、SLF4J API 2.0.17、JUnit Jupiter、AssertJ、Mockito、Awaitility、JMH、Spring Boot 3.5/4.x。

## Global Constraints

- 所有需求以 `docs/stow-design.md`、`docs/stow-api-contract.md` 和 `docs/stow-persistence-contract.md` 为准。
- Maven 坐标固定为 `io.github.cocosip:stow-core` 与 `io.github.cocosip:stow-spring-boot-starter`。
- 根包固定为 `io.github.cocosip.stow`；JDK 编译参数固定为 `--release 21`。
- `stow-core` 不依赖 Spring、CDI、Guice、Micrometer、Actuator 或具体日志 Provider。
- 核心日志依赖固定为 `org.slf4j:slf4j-api:2.0.17`，并提供 1.7.36 宿主兼容验证。
- 公开 API 使用同步阻塞模型，全部线程安全并响应线程中断。
- 只发布两个正式 JAR；samples、benchmarks 和兼容测试模块设置 `maven.deploy.skip=true`。
- 每个实现任务先提交失败测试，再写最小实现；不得通过 sleep、重试测试或放宽断言掩盖竞态。
- 所有磁盘兼容变化先修改 persistence contract 并增加迁移/恢复测试。
- 在全部任务和完整门禁通过前，不发布或声明 1.0.0 完成。

---

## File Map

```text
pom.xml
.mvn/wrapper/maven-wrapper.properties
mvnw
mvnw.cmd
stow-core/src/main/java/io/github/cocosip/stow/
  Stow.java, StowBuilder.java, StowRuntime.java, RuntimeState.java
  api/*.java, model/*.java, config/*.java, exception/*.java, spi/*.java
  internal/runtime/*.java
  internal/filesystem/*.java
  internal/sqlite/*.java
  internal/tenant/*.java
  internal/quota/*.java
  internal/journal/*.java
  internal/projection/*.java
  internal/scheduler/*.java
  internal/cleanup/*.java
  internal/recovery/*.java
  internal/watcher/*.java
  internal/statistics/*.java
stow-core/src/test/java/io/github/cocosip/stow/**
stow-spring-boot-starter/src/main/java/io/github/cocosip/stow/spring/**
stow-spring-boot-starter/src/test/java/io/github/cocosip/stow/spring/**
samples/stow-sample-console/**
samples/stow-sample-spring-boot/**
benchmarks/**
```

每个生产类只承担一个职责。单个文件超过约 400 行时，在同一任务内按 codec/store/service/state 拆分，不创建通用 `Utils` 或 `Manager` 大杂烩。

### Task 1: Maven reactor and quality baseline

**Files:**
- Create: `pom.xml`, `.mvn/wrapper/maven-wrapper.properties`, `mvnw`, `mvnw.cmd`
- Create: `stow-core/pom.xml`, `stow-spring-boot-starter/pom.xml`
- Create: `samples/stow-sample-console/pom.xml`, `samples/stow-sample-spring-boot/pom.xml`, `benchmarks/pom.xml`
- Modify: `.gitignore`
- Test: `stow-core/src/test/java/io/github/cocosip/stow/BuildBaselineTest.java`

**Interfaces:** Produces a Java 21 Maven reactor and dependency/plugin management used by every later task.

- [x] Write `BuildBaselineTest` asserting `Runtime.version().feature() == 21` and that `org.slf4j.LoggerFactory` loads without a bundled Provider assertion.
- [x] Run `mvnw -pl stow-core test -Dtest=BuildBaselineTest`; expect failure because reactor/modules do not exist.
- [x] Create the reactor with exact managed versions: SLF4J 2.0.17, sqlite-jdbc 3.50.3.0, Jackson 2.20.0, JUnit 5.13.4, AssertJ 3.27.6, Mockito 5.20.0 and Awaitility 4.3.0. Configure compiler, Surefire/Failsafe, JaCoCo, Spotless, SpotBugs, source and Javadoc plugins.
- [x] Centralize the reactor release version with `${revision}`; make every child inherit it, keep all dependency and plugin versions in the root POM, and flatten the two published artifacts to concrete release versions as specified in `docs/build-version-management.md`.
- [x] Add `target/`, `.classpath`, `.project`, `.settings/`, `*.iml` and benchmark results to `.gitignore`; preserve existing entries.
- [x] Run `mvnw verify`; expect all empty modules and `BuildBaselineTest` to pass with no concrete logging binding in `stow-core` dependency tree.
- [x] Commit: `🏗️ build(maven): establish Java 21 reactor`

### Task 2: Public models, exceptions, configuration, and API

**Files:**
- Create: `stow-core/src/main/java/io/github/cocosip/stow/{api,model,config,exception,spi}/*.java`
- Test: `stow-core/src/test/java/io/github/cocosip/stow/config/StowConfigurationTest.java`
- Test: `stow-core/src/test/java/io/github/cocosip/stow/model/PublicModelTest.java`

**Interfaces:**
- Produces the exact public signatures in `docs/stow-api-contract.md`.
- Produces `StowConfiguration.builder().build()`, immutable records, enums, `StowException#errorCode()` and SPI contracts.

- [x] Write parameterized configuration tests for every default-value row and invalid zero/negative/range/path combination.
- [x] Write model tests proving defensive collection copies, required field validation, 4096-character error truncation and 255/32-character filename/extension limits.
- [x] Run `mvnw -pl stow-core test -Dtest=StowConfigurationTest,PublicModelTest`; expect compilation failure for missing types.
- [x] Implement public records, enums, interfaces and exceptions exactly as the API contract; use compact constructors and immutable copies.
- [x] Run the focused tests, then `mvnw -pl stow-core test`; expect pass.
- [x] Commit: `✨ feat(core): define public API and configuration contracts`

### Task 3: Runtime lifecycle and composition root

**Files:**
- Create: `stow-core/src/main/java/io/github/cocosip/stow/Stow.java`, `StowBuilder.java`, `StowRuntime.java`, `RuntimeState.java`
- Create: `stow-core/src/main/java/io/github/cocosip/stow/internal/runtime/DefaultStowRuntime.java`, `DefaultStowRuntimeFactory.java`, `RuntimeDirectoryLock.java`, `ManagedBackgroundService.java`
- Test: `stow-core/src/test/java/io/github/cocosip/stow/internal/runtime/DefaultStowRuntimeTest.java`

**Interfaces:** Consumes `StowConfiguration`; produces `Stow.builder()`, `Stow.open()`, lifecycle state and resource ownership rules.

- [x] Write tests for `NEW -> STARTING -> RUNNING -> STOPPING -> TERMINATED`, failed start, duplicate start, idempotent close, reverse-order close and caller-owned executor preservation.
- [x] Add a test opening two runtimes against the same roots; expect the second to fail with `RuntimeDirectoryLockedException`.
- [x] Run the focused test; expect failure for missing runtime.
- [x] Implement the directory lock and lifecycle without starting threads in `build()`; use `AtomicReference<RuntimeState>` and an ordered resource stack.
- [x] Run `mvnw -pl stow-core test -Dtest=DefaultStowRuntimeTest`; expect pass and no leaked `stow-*` threads.
- [x] Commit: `✨ feat(runtime): add framework-neutral lifecycle`

### Task 4: SQLite foundation and tenant persistence

**Files:**
- Create: `internal/sqlite/SqliteConnectionFactory.java`, `SqliteSchemaManager.java`, `AtomicJsonFile.java`
- Create: `internal/tenant/JsonTenantRepository.java`, `DefaultTenantManager.java`, `TenantDocument.java`
- Test: `internal/sqlite/SqliteConnectionFactoryTest.java`, `internal/tenant/DefaultTenantManagerTest.java`

**Interfaces:** Produces safe PRAGMA setup, schema version checks, atomic JSON persistence and the public `TenantManager`.

- [x] Write tests for PRAGMA whitelist, per-tenant path isolation, atomic JSON replacement, auto-create off/on, enable/disable persistence and concurrent create idempotency.
- [x] Run focused tests; expect missing-class failure.
- [x] Implement direct JDBC connections with WAL/NORMAL defaults and `PRAGMA user_version`; implement `tenants.json` under an exclusive repository lock.
- [x] Inject an I/O failure between temp-file force and move; prove the old tenant document remains readable.
- [x] Run focused tests and `mvnw -pl stow-core test`; expect pass.
- [x] Commit: `✨ feat(tenant): persist isolated tenant lifecycle`

### Task 5: Local file-system volumes and selection

**Status (2026-09-18):** Complete. Final independent review approved after commits `c33863d`, `5fe89f2` and `12c468c`; `stow-core clean verify` passed with 115 tests, 0 failures, 5 environment-dependent skips, clean Spotless and 0 SpotBugs findings.

**Files:**
- Create: `internal/filesystem/PathPolicy.java`, `FileKeyGenerator.java`, `LocalFileSystemVolume.java`, `DefaultStorageVolumeProvider.java`, `VolumeRegistry.java`, `PowerOfTwoVolumeSelector.java`
- Test: `internal/filesystem/PathPolicyTest.java`, `LocalFileSystemVolumeTest.java`, `PowerOfTwoVolumeSelectorTest.java`

**Interfaces:** Implements `StorageVolume`; produces validated shard paths, atomic write/read/delete/move, health/capacity probes and ordered write candidates.

- [x] Write Windows/Linux-safe tests for traversal, reserved tenant names, extension sanitizing, sharding depth 0-3, symlink escape, temp cleanup and atomic visibility.
- [x] Write selector tests proving unhealthy/full volumes are excluded and repeated choices distribute writes.
- [x] Run focused tests; expect failure.
- [x] Implement file keys as 32 lowercase hex characters, same-directory temp writes, optional `FileChannel.force(true)`, atomic move and cached health/capacity.
- [x] Run focused tests on real temporary directories; expect pass.
- [x] Commit: `✨ feat(filesystem): add durable multi-volume storage` (`c33863d`), with path-confinement hardening in `5fe89f2` and cross-provider regression coverage in `12c468c`.
- [x] Complete independent review; result: `Spec compliance: PASS`, `Quality: APPROVED`.

### Task 6: Tenant and directory quota reservations

**Status (2026-09-18):** Complete. Implementation committed in `c283124` with review fixes in `8f29948`; final focused verification passed 19/19 tests and `stow-core clean verify` passed with 126 tests, 0 failures, 5 environment-dependent skips, clean Spotless and 0 SpotBugs findings. Independent review: all five important findings addressed; one minor directory-concurrency coverage suggestion remains deferred.

**Files:**
- Create: `internal/quota/SqliteQuotaRepository.java`, `QuotaReservation.java`, `DefaultTenantQuotaManager.java`, `DefaultDirectoryQuotaManager.java`
- Test: `internal/quota/SqliteQuotaRepositoryTest.java`, `QuotaConcurrencyTest.java`

**Interfaces:** Produces atomic `reserve(fileKey, directory)`, `consume(eventId, sequenceNumber, reservationId)`, `rollback(reservationId)`, `release(eventId, sequenceNumber, fileKey, logicalDirectory)` and public quota managers. Event methods persist the real journal sequence, and release receives the normalized logical directory because a consumed reservation no longer exists to supply it.

- [x] Create schema tests matching the exact `tenant_quota`, `directory_quotas`, `quota_reservations` and `applied_quota_events` DDL in the persistence contract.
- [x] Write concurrent boundary tests where N writers compete for one remaining slot; assert exactly one reservation succeeds.
- [x] Run focused tests; expect failure.
- [x] Implement optimistic row-version transactions, logical-directory normalization, unlimited zero limits and idempotent consume/rollback/release.
- [x] Add crash-style reopen tests proving reservations survive and can be reconciled.
- [x] Commit: `✨ feat(quota): add durable atomic reservations` (`c283124`), with review fixes in `8f29948`.
- [x] Complete independent review; five important findings addressed, one minor coverage suggestion deferred.

### Task 7: Journal codecs and format detection

**Status (2026-09-18):** Complete. Binary V1 and JsonLines V1 codecs, canonical JSON/CRC validation, format detection, and golden fixtures committed in `8e2f120`; focused and `stow-core` tests pass.

**Files:**
- Create: `internal/journal/BinaryV1JournalCodec.java`, `JsonLinesJournalCodec.java`, `JournalFrame.java`, `JournalFormatDetector.java`, `QueueEventJson.java`
- Test: `internal/journal/BinaryV1JournalCodecTest.java`, `JsonLinesJournalCodecTest.java`, `JournalFormatDetectorTest.java`

**Interfaces:** Implements `JournalCodec`; produces exact Binary V1 and JsonLines V1 bytes from the persistence contract.

- [x] Add golden-byte tests for magic, big-endian lengths, sequence and CRC32; store fixtures under `stow-core/src/test/resources/journal/v1/`.
- [x] Add round-trip tests for all eight event types, null optional fields, Unicode error summaries, payload limit and unknown schema.
- [x] Run focused tests; expect failure.
- [x] Implement deterministic Jackson serialization and strict frame validation without accepting trailing bytes.
- [x] Run focused tests and verify golden fixtures are stable across two clean builds.
- [x] Commit: `✨ feat(journal): define versioned queue codecs` (`8e2f120`)

### Task 8: Durable per-tenant journal engine

**Status (2026-09-18):** Complete. Durable per-tenant writers, ACK modes, ordering, bounded queues, tail repair, state recovery, format detection, compaction suffix recovery, and tenant isolation committed in `7ef7cf6`; focused and `stow-core` tests pass.

**Files:**
- Create: `internal/journal/FileQueueEventJournal.java`, `TenantJournalWriter.java`, `JournalStateStore.java`, `JournalScanner.java`, `JournalReadBatch.java`
- Test: `internal/journal/FileQueueEventJournalTest.java`, `JournalCrashRecoveryTest.java`

**Interfaces:** Implements `QueueEventJournal.append`, `appendBatch`, `readBatch`, `tailOffset`, `baseOffset`, `tenantIds`, `compact` and `flush`.

- [x] Write tests for strict per-tenant ordering, cross-tenant parallelism, DURABLE/BALANCED/ASYNC acknowledgment, bounded queue rejection and graceful drain.
- [x] Write corrupt-tail tests for partial frame, bad final CRC, middle corruption, state loss and sequence gap.
- [x] Run focused tests; expect failure.
- [x] Implement one bounded writer per active tenant, micro-batching, idle close, state debounce, file locking and safe tail truncation.
- [x] Verify middle corruption/sequence conflict isolates the tenant and never auto-skips.
- [x] Commit: `✨ feat(journal): add durable tenant event logs` (`7ef7cf6`)

### Task 9: Metadata projection, reducer, and active cache

**Status (2026-09-18):** Complete. Metadata schema, legal event reducer transitions, independent quota/idempotency application, cursor persistence, active cache, and projection service committed in `039c03d`; focused and `stow-core` tests pass.

**Files:**
- Create: `internal/projection/QueueEventReducer.java`, `SqliteMetadataProjectionStore.java`, `ActiveFileCache.java`, `ProjectionCursorStore.java`, `QueueProjectionService.java`
- Test: `internal/projection/QueueEventReducerTest.java`, `QueueProjectionServiceTest.java`, `ProjectionIdempotencyTest.java`

**Interfaces:** Consumes journal batches and quota reservations; produces metadata schema, active rows, applied-events idempotency and cursor progress.

- [x] Write a transition matrix test covering every row in persistence contract section 11 plus every illegal predecessor.
- [x] Write replay tests for duplicate event ID, sequence collision, metadata commit before quota, quota commit before cursor, batch rollback and first-access cache load.
- [x] Run focused tests; expect failure.
- [x] Implement schema version 1 exactly, idempotent two-database reducer application, both applied-events tables and cursor-after-both-commits ordering.
- [x] Run focused tests with real SQLite; expect pass and deterministic tenant isolation.
- [x] Commit: `✨ feat(projection): build replayable SQLite state` (`039c03d`)

### Task 10: Storage write/read pipeline

**Status (2026-09-19):** Complete. `DefaultStoragePool`, repeatable/non-repeatable content sources, atomic physical writes, quota compensation, journal acceptance, projection, ownership-safe reads, and capacity aggregation committed in `9f2e578`; focused tests pass.

**Files:**
- Create: `internal/scheduler/DefaultStoragePool.java`, `CountingInputStream.java`, `WriteCompensation.java`
- Test: `internal/scheduler/StorageWriteReadTest.java`, `StorageWriteFailureTest.java`

**Interfaces:** Implements StoragePool write/read/info/location/capacity; consumes tenant, quota, volume, journal and projection services.

- [x] Write happy-path tests for repeatable path/byte-array ContentSource, non-repeatable InputStream, extensions, logical directories, read ownership and capacity totals.
- [x] Write failure-injection tests at reservation, physical write, atomic move, ACCEPTED append and projection enqueue boundaries.
- [x] Run focused tests; expect failure.
- [x] Implement both StoragePool write overloads and the six persistence boundaries; only retry another volume when `ContentSource.repeatable()` is true.
- [x] Reopen runtime after each injected failure and assert no silent file loss or released quota while a file remains.
- [x] Commit: `✨ feat(storage): implement durable write and read paths` (`9f2e578`)

### Task 11: Leasing, retry, completion, and timeout recovery

**Files:**
- Create: `internal/scheduler/FileScheduler.java`, `RetryDelayCalculator.java`, `StripedFileLock.java`, `ProcessingTimeoutRecovery.java`
- Test: `internal/scheduler/FileSchedulerTest.java`, `LeaseRaceTest.java`, `ProcessingTimeoutRecoveryTest.java`

**Interfaces:** Completes StoragePool claimNext/claimBatch/complete/fail/status using UUID leases and event transitions.

- [x] Write tests for unique single/batch claims, availableAt filtering, exponential cap, permanent failure and empty queue.
- [x] Write race tests for duplicate complete, complete-vs-fail, stale lease, cross-tenant lease, released same lease and different lease.
- [x] Run focused tests; expect failure.
- [x] Implement conditional SQLite claim, per-file striped serialization and rollback when journal append fails.
- [x] Add timeout tests proving old lease invalidation and PROCESSING_TIMED_OUT replay.
- [x] Commit: `✨ feat(scheduler): enforce lease-safe queue processing` (`57b36aa`); race assertion fix (`9e0fb94`)

### Task 12: Snapshot, compaction, rebuild, and database recovery

**Files:**
- Create: `internal/projection/ProjectionSnapshotStore.java`, `ProjectionMaintenanceService.java`
- Create: `internal/recovery/DatabaseHealthService.java`, `DatabaseRecoveryService.java`
- Test: `internal/projection/SnapshotCompactionTest.java`, `internal/recovery/DatabaseRecoveryServiceTest.java`

**Interfaces:** Implements QueueProjectionMaintenance and database maintenance results.

- [ ] Write deterministic snapshot/CRC tests and reject out-of-range snapshot offsets.
- [ ] Write compaction crash tests before/after temp force, log replace and state replace.
- [ ] Run focused tests; expect failure.
- [ ] Implement catch-up gating, snapshot verification, exclusive compaction and snapshot+tail rebuild.
- [ ] Corrupt metadata.db and quotas.db separately; prove metadata rebuilds from journal and quota rebuilds from active metadata.
- [ ] Commit: `✨ feat(recovery): add projection and database rebuilds`

### Task 13: Cleanup, two-phase delete, dead letter, and orphan recovery

**Files:**
- Create: `internal/cleanup/DefaultStorageMaintenance.java`, `CompletedFileReaper.java`, `PermanentFailureReaper.java`, `JunkFileCleaner.java`, `RetiredVolumeCleaner.java`
- Create: `internal/recovery/OrphanFileRecovery.java`
- Test: `internal/cleanup/StorageMaintenanceTest.java`, `internal/recovery/OrphanFileRecoveryTest.java`

**Interfaces:** Implements all StorageMaintenance operations and scheduled cleanup hooks.

- [ ] Write tests for DELETE_REQUESTED/delete/DELETE_SUCCEEDED ordering, missing file idempotency and failure retry.
- [ ] Test KEEP, DELETE, MOVE_TO_DEAD_LETTER, same-filesystem validation and event failure after move.
- [ ] Run focused tests; expect failure.
- [ ] Implement batched cleanup, empty/junk/backup cleanup, retired-volume policy, checkpoint/VACUUM and statistics.
- [ ] Add orphan tests for physical file without event, event without projection, metadata without physical file and maximum scan limit.
- [ ] Commit: `✨ feat(maintenance): complete cleanup and orphan recovery`

### Task 14: Persistent file watcher subsystem

**Files:**
- Create: `internal/watcher/DefaultFileWatcherManager.java`, `WatcherConfigurationStore.java`, `ImportedFileHistory.java`, `WatcherScanner.java`, `DefaultFileWatcherAutoManager.java`
- Test: `internal/watcher/FileWatcherManagerTest.java`, `WatcherScannerTest.java`, `FileWatcherAutoManagerTest.java`

**Interfaces:** Implements watcher manager/options/auto-manager APIs and imports through StoragePool only.

- [ ] Write tests for configuration persistence, register/update/remove/enable/disable, single/multi-tenant mapping and root discovery.
- [ ] Write scan tests for glob, recursion, size/age/stability, bounded concurrency and DELETE/MOVE/KEEP.
- [ ] Run focused tests; expect failure.
- [ ] Implement polling scans on virtual threads, atomic configuration/history persistence, debounce and prune throttle.
- [ ] Inject post-action failure after successful import; prove history prevents duplicate import and next scan retries only the post-action.
- [ ] Commit: `✨ feat(watcher): add persistent directory imports`

### Task 15: Statistics, diagnostics, background scheduling, and health

**Files:**
- Create: `internal/statistics/NoopStatisticsRecorder.java`, `WindowedStatisticsRecorder.java`, `DefaultStatisticsReader.java`
- Create: `internal/runtime/BackgroundServiceCoordinator.java`, `DefaultRuntimeHealth.java`
- Test: `internal/statistics/WindowedStatisticsRecorderTest.java`, `internal/runtime/BackgroundServiceCoordinatorTest.java`

**Interfaces:** Produces bounded windowed statistics, diagnostics snapshots, health aggregation and runtime-owned schedules.

- [ ] Write Clock-driven tests for bucket boundaries, retention, max-series rejection, dimension switches and no-op behavior.
- [ ] Write service-order tests for projector, timeout recovery, cleanup, orphan recovery, watcher and statistics output.
- [ ] Run focused tests; expect failure.
- [ ] Implement LongAdder-based aggregation with immutable snapshots and named health components.
- [ ] Verify no statistic or metric label contains file key, filename or physical path.
- [ ] Commit: `✨ feat(observability): add bounded statistics and health`

### Task 16: Spring Boot starter

**Files:**
- Create: `stow-spring-boot-starter/src/main/java/io/github/cocosip/stow/spring/StowAutoConfiguration.java`, `StowProperties.java`, `StowLifecycle.java`
- Create: conditional `StowHealthContributor.java`, `StowMeterBinder.java`
- Create: `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- Test: `StowAutoConfigurationTest.java`, `StowPropertiesTest.java`, `StowLifecycleTest.java`

**Interfaces:** Converts `stow.*` properties into core configuration, owns one runtime Bean and forwards public service Beans.

- [ ] Write ApplicationContextRunner tests for defaults, invalid properties, user Clock override, disabled Actuator/Micrometer and clean shutdown.
- [ ] Run focused starter tests; expect failure.
- [ ] Implement auto-configuration without registering internal core components as Beans and without adding a logging Provider.
- [ ] Add compatibility test executions against the lowest Spring Boot 3.5 line and current 4.x line.
- [ ] Run `mvnw -pl stow-spring-boot-starter -am verify`; expect pass.
- [ ] Commit: `✨ feat(spring): add Boot lifecycle adapter`

### Task 17: Samples, configuration reference, and Javadocs

**Files:**
- Create: console and Spring Boot sample sources/configuration
- Create: `docs/configuration-reference.md`, `docs/operations-and-recovery.md`
- Create: `README.md`
- Modify: `docs/README.md`
- Test: sample smoke tests under each sample module

**Interfaces:** Demonstrates all public entry points without internal imports.

- [ ] Write smoke tests that start each sample in a temp directory, write, claim, complete, close and reopen.
- [ ] Run sample tests; expect failure before sample code exists.
- [ ] Implement console/Spring samples and document every configuration property/default plus operator replay/snapshot/rebuild commands.
- [ ] Run Javadoc with `-Werror` equivalent doclint and scan samples for `internal` imports; expect none.
- [ ] Commit: `📝 docs(samples): document complete Stow operation`

### Task 18: Crash, concurrency, compatibility, benchmark, and release gates

**Files:**
- Create: `stow-core/src/test/java/io/github/cocosip/stow/integration/*IT.java`
- Create: `stow-core/src/test/java/io/github/cocosip/stow/compat/Slf4jCompatibilityTest.java`
- Create: `benchmarks/src/main/java/io/github/cocosip/stow/benchmarks/*.java`
- Create: CI workflows for Windows/Linux JDK 21 and compatibility profiles

**Interfaces:** Provides final evidence for every completion condition; changes production code only for defects exposed by these tests.

- [ ] Implement a forked-JVM crash harness for every row in persistence contract section 15 and assert deterministic recovery after process termination.
- [ ] Add multi-producer/multi-consumer stress, file-handle/thread leak, long-run WAL/journal growth and bounded-queue backpressure tests.
- [ ] Run `mvnw verify`, `mvnw -Pslf4j1-compat verify` and `mvnw -Pspring-boot-compat verify`; all must pass.
- [ ] Run `mvnw -Pbenchmarks package`; capture baseline results without defining correctness thresholds from benchmark noise.
- [ ] Run dependency vulnerability/license checks, dependency tree scan for logging bindings, API compatibility check, source/Javadoc package and reproducible-build comparison.
- [ ] Compare the completed code against every row in `docs/stow-design.md` section 25 and record command evidence in `docs/release-verification.md`.
- [ ] Commit: `✅ test(release): prove complete Stow 1.0 behavior`

## Final Verification

Run from repository root in this order:

```powershell
.\mvnw.cmd spotless:check
.\mvnw.cmd verify
.\mvnw.cmd -Pslf4j1-compat verify
.\mvnw.cmd -Pspring-boot-compat verify
.\mvnw.cmd -Pbenchmarks package
git diff --check
git status --short
```

Expected result: every command exits 0, the final status contains only intentionally uncommitted benchmark output or is empty, and `docs/release-verification.md` maps all design requirements to passing tests/commands. Any failure keeps 1.0 incomplete.

## Spec Coverage Check

| Design area | Implementing tasks |
| --- | --- |
| Maven coordinates, Java 21, dependency policy | 1 |
| Public API, models, configuration, exceptions, SPI | 2 |
| Framework-neutral DI and lifecycle | 3 |
| SQLite foundation and tenants | 4 |
| Volumes, paths, health, capacity and sharding | 5 |
| Tenant/directory quota and compensation | 6 |
| Journal formats, CRC and format detection | 7 |
| ACK modes, batching, ordering and bad-tail repair | 8 |
| Reducer, SQLite projection and active cache | 9 |
| Write/read/info/location/capacity | 10 |
| Claim, lease, retry, completion and timeout | 11 |
| Snapshot, compaction, replay and database rebuild | 12 |
| Cleanup, dead letter, retired volume and orphan recovery | 13 |
| Watcher persistence, scanning and auto-management | 14 |
| Statistics, diagnostics, schedules and health | 15 |
| Spring Boot, Actuator and Micrometer adaptation | 16 |
| Samples and operator documentation | 17 |
| Crash, concurrency, compatibility, benchmarks and release evidence | 18 |
