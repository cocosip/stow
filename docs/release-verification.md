# Stow 1.0 Release Verification

This document records the evidence used for the Stow 1.0 release gates. The
capability list follows section 25 of `docs/stow-design.md`; the tests named
below run in `stow-core` unless a module is stated explicitly.

## Alignment Matrix Evidence

| Section 25 capability | Implementation | Evidence |
| --- | --- | --- |
| Tenant lifecycle and isolation | `internal.tenant`, `TenantManager` | `DefaultTenantManagerTest`; `RuntimeConcurrencyIT` |
| Tenant and directory quotas | `internal.quota` | `SqliteQuotaRepositoryTest`; `QuotaConcurrencyTest` |
| Volumes, health, capacity, sharding | `internal.filesystem` | `LocalFileSystemVolumeTest`; `PowerOfTwoVolumeSelectorTest` |
| Write, read, info, location | `StoragePool` and scheduler | `StorageWriteReadTest`; `RuntimeConcurrencyIT` |
| Claims and leases | `internal.scheduler` | `FileSchedulerTest`; `LeaseRaceTest` |
| Retry, backoff, permanent failure | `internal.scheduler`, cleanup | `RetryDelayCalculatorTest`; `ProcessingTimeoutRecoveryTest`; `StorageMaintenanceTest` |
| Queue events and journal formats | `internal.journal` | `JournalCodecTest`; `FileQueueEventJournalTest` |
| CRC, sequences, tail repair | `internal.journal` | `JournalCodecTest`; `JournalCrashRecoveryTest` |
| SQLite projections and active cache | `internal.projection`, `internal.sqlite` | `QueueProjectionServiceTest`; `QueueEventReducerTest`; `ProjectionIdempotencyTest` |
| Cursor, snapshot, compaction, rebuild | `internal.projection`, `internal.recovery` | `SnapshotCompactionTest`; `DatabaseRecoveryServiceTest` |
| Two-phase deletion and dead-letter | `internal.cleanup` | `StorageMaintenanceTest` |
| Timeout reclaim and orphan recovery | `internal.recovery`, `internal.scheduler` | `ProcessingTimeoutRecoveryTest`; `OrphanFileRecoveryTest` |
| Database health, rebuild, optimization | `internal.recovery`, `internal.sqlite` | `DatabaseRecoveryServiceTest`; `SqliteConnectionFactoryTest` |
| Quota reconciliation and cleanup | `internal.quota`, `internal.cleanup` | `SqliteQuotaRepositoryTest`; `StorageMaintenanceTest` |
| Watcher configuration and automation | `internal.watcher` | `FileWatcherManagerTest`; `FileWatcherAutoManagerTest`; `WatcherScannerTest` |
| Runtime statistics and diagnostics | `internal.statistics`, runtime health | `WindowedStatisticsRecorderTest`; `BackgroundServiceCoordinatorTest` |
| Host lifecycle integration | `DefaultStowRuntime`, Spring Boot starter | `DefaultStowRuntimeTest`; starter `verify`; sample smoke tests |

## Release Commands

Run from the repository root with JDK 21:

```powershell
.\mvnw.cmd -B -ntp spotless:check
.\mvnw.cmd -B -ntp verify
.\mvnw.cmd -B -ntp -Pslf4j1-compat verify
.\mvnw.cmd -B -ntp -Pspring-boot-compat verify
.\mvnw.cmd -B -ntp -Pbenchmarks package
```

The integration tests provide the process and concurrency evidence required by
the design:

```powershell
.\mvnw.cmd -B -ntp -pl stow-core `
  '-Dtest=CrashRecoveryIT,RuntimeConcurrencyIT,Slf4jCompatibilityTest' `
  '-Dsurefire.failIfNoSpecifiedTests=false' test
```

`CrashRecoveryIT` starts a forked JVM, terminates it while writes are in
flight, reopens the same roots, and checks every recorded key. Both integration
tests use JUnit-managed isolated temporary directories so parallel IDE runs do
not share runtime roots or leave test data in the repository. The crash test
also waits for Windows file-lock release before reopening the runtime.
`RuntimeConcurrencyIT` exercises four producers and four consumers and verifies
unique claims and completion of every written file.

## Additional Release Checks

The following checks are part of the release review and must be run against a
clean checkout or the verified build output:

```powershell
git diff --check
git status --short
.\mvnw.cmd -B -ntp -pl stow-core dependency:tree |
  Select-String 'logback|log4j|slf4j-simple|slf4j-nop'
.\mvnw.cmd -B -ntp -Pbenchmarks -pl benchmarks -am package
```

The core dependency tree is expected to contain `slf4j-api` only; concrete
bindings are supplied by applications or the benchmark/sample modules. The
reactor build also produces sources and Javadoc for the two publishable
artifacts. The benchmark module and samples set `maven.deploy.skip=true` and
are evidence-only modules.

## Evidence Boundary

Benchmark numbers are recorded as performance baselines and are not
correctness thresholds. Vulnerability/license scanners, API compatibility
reports, and reproducible-build comparisons are release-environment checks;
their output belongs to the CI/release job artifacts and does not alter the
runtime contract documented above.
