# Stow

Stow is a Java 21, multi-tenant file storage pool for local filesystems and
mounted filesystems. It provides durable file placement, tenant isolation,
processing leases, quotas, recovery, maintenance, and a framework-neutral API.
Spring Boot integration is provided by a separate starter module.

The project is currently versioned as `0.1.0-SNAPSHOT`. Stable architecture,
API, persistence, operations, and release guidance is collected in the
[`docs/`](docs/) documentation index.

## Features

- Durable writes to one or more sharded storage volumes.
- Tenant-scoped metadata, queues, quotas, and physical paths.
- Append-only queue journals with SQLite projections that can be rebuilt.
- Claim, complete, fail, retry, timeout, cleanup, and orphan recovery flows.
- Persistent directory watchers and automatic import management.
- Runtime health, statistics, maintenance, and Spring Boot Actuator adapters.
- A core module that depends on the SLF4J 2 API, not on a logging framework.

Stow is not an object-storage client and does not allow callers to choose an
arbitrary physical path. NFS, SMB, PVC, and similar systems can be used after
they are mounted by the operating system.

## Requirements

- OpenJDK 21
- Maven 3.9 or newer

The repository includes Maven Wrapper scripts. Use `mvnw.cmd` on Windows and
`./mvnw` on Linux or macOS.

## Dependency

The core dependency is framework-neutral:

```xml
<dependency>
  <groupId>io.github.cocosip</groupId>
  <artifactId>stow-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

For Spring Boot applications use the starter instead:

```xml
<dependency>
  <groupId>io.github.cocosip</groupId>
  <artifactId>stow-spring-boot-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Install the current checkout into the local Maven repository before consuming
the snapshot from another project:

```bash
./mvnw install
```

`stow-core` exposes `slf4j-api` 2.x only. Applications must select one
provider, for example `slf4j-simple`, Logback, or Log4j2. The core module does
not install a provider and therefore does not decide how application logs are
formatted or routed.

## Quick Start: Write and Read

The following is a complete framework-neutral operation. A file is written,
claimed and completed, read back, and then read again after a runtime restart.

```java
import io.github.cocosip.stow.Stow;
import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.api.ContentSources;
import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.WriteOptions;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

StowConfiguration configuration = StowConfiguration.builder()
        .metadataDirectory(Path.of("data/metadata"))
        .quotaDirectory(Path.of("data/quota"))
        .queueDirectory(Path.of("data/queue"))
        .watcherDirectory(Path.of("data/watchers"))
        .autoCreateTenants(true)
        .volumes(List.of(new VolumeConfiguration(
                "primary", Path.of("data/volume"), 2, 65_536, true)))
        .build();

String fileKey;
try (StowRuntime runtime = Stow.open(configuration)) {
    TenantContext tenant = runtime.tenantManager().get("tenant-a");
    fileKey = runtime.storagePool().write(
            tenant,
            ContentSources.of("hello from Stow".getBytes(StandardCharsets.UTF_8)),
            WriteOptions.ofOriginalFileName("hello.txt"));

    var claimed = runtime.storagePool().claimNext(tenant).orElseThrow();
    runtime.storagePool().complete(claimed.lease());

    try (InputStream input = runtime.storagePool().read(tenant, fileKey)) {
        String content = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        System.out.println(content);
    }
}

try (StowRuntime reopened = Stow.open(configuration)) {
    TenantContext tenant = reopened.tenantManager().get("tenant-a");
    try (InputStream input = reopened.storagePool().read(tenant, fileKey)) {
        System.out.println(new String(input.readAllBytes(), StandardCharsets.UTF_8));
    }
}
```

`Stow.open` starts the runtime. `StowRuntime` is `AutoCloseable`, so
try-with-resources releases the runtime lock, background services, journal
writers, database connections, and storage volumes. A caller that uses
`Stow.builder()` instead of `Stow.open()` must call `start()` explicitly.

## Configuration

The framework-neutral entry point is `StowConfiguration.builder()`. Paths are
converted to absolute normalized paths during validation.

| Area | Builder method | Meaning |
| --- | --- | --- |
| Metadata | `metadataDirectory(Path)` | Tenant metadata and runtime lock |
| Quota | `quotaDirectory(Path)` | SQLite quota databases |
| Queue | `queueDirectory(Path)` | Per-tenant journals and projection state |
| Watchers | `watcherDirectory(Path)` | Watcher configuration and import history |
| Tenants | `autoCreateTenants(boolean)` | Create a tenant on first lookup |
| Tenants | `defaultQuota(long)` | Default file-count quota; `0` means unlimited |
| Tenants | `preconfiguredTenants(List<String>)` | Tenants ensured during startup |
| Volumes | `volumes(List<VolumeConfiguration>)` | Ordered physical storage volumes |
| Persistence | `backgroundPersistence`, `journalAckMode`, `journalFormat` | Journal durability and encoding |
| Projection | `journalProjectionEnabled`, `projection*` | SQLite projection scheduling |
| Recovery | `snapshot*`, `compaction*`, `orphanRecovery*` | Replay and recovery behavior |
| Cleanup | `cleanup*`, `processingTimeout`, `completedRetention` | Retention and maintenance |
| Statistics | `statistics*` | Windowed runtime measurements |

Each `VolumeConfiguration` contains an id, mount path, sharding depth from `0`
to `3`, a positive buffer size, and the `forceFlushAfterWrite` flag. The full
property list, defaults, validation rules, and Spring Boot names are in the
[API and configuration contract](docs/stow-api-contract.md).

## Public API

The main services are exposed by `StowRuntime`:

| Service | Typical operations |
| --- | --- |
| `StoragePool` | `write`, `read`, `findFileInfo`, `findFileLocation`, `status`, `totalCapacity` |
| `TenantManager` | Create, find, enable, disable, and list tenants |
| `TenantQuotaManager` | Read and change tenant file-count limits |
| `DirectoryQuotaManager` | Read and change logical-directory limits |
| `QueueProjectionMaintenance` | Replay, snapshot, compact, and rebuild projections |
| `StorageMaintenance` | Cleanup, dead-letter handling, and orphan recovery |
| `FileWatcherManager` | Register, update, enable, disable, scan, and remove watchers |
| `StatisticsReader` | Query operation counters and latency windows |
| `health()` | Aggregate component health for diagnostics |

`write` returns a generated `fileKey`. The returned key is the stable handle
for `read`, metadata lookup, location lookup, and status lookup. Queue workers
call `claimNext` or `claimBatch`, then pass the exact `ProcessingLease` to
`complete` or `fail`; a stale or mismatched lease is rejected.

Watcher source cleanup is durable and asynchronous. It is active only when
`sourceCleanup.enabled`, the global watcher option, and at least one watcher
configuration are all enabled. If any condition is false, Stow does not open
the source-cleanup database, reclaim reservations, prune records, or run
`VACUUM`. A bounded cleanup queue defers new imports when full instead of
importing files whose DELETE or MOVE action cannot be recovered.

## Spring Boot integration

Add the starter to a Spring Boot application. It brings in `stow-core`, binds
`stow.*` properties, creates and starts one `StowRuntime`, and closes it during
application shutdown. It also exposes the public Stow services as beans.
Actuator health and Micrometer meters are registered only when those optional
Spring Boot modules are on the classpath.

Maven:

```xml
<dependency>
  <groupId>io.github.cocosip</groupId>
  <artifactId>stow-spring-boot-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Minimal `application.yml`:

```yaml
stow:
  paths:
    metadata-directory: ./data/metadata
    quota-directory: ./data/quota
    queue-directory: ./data/queue
    watcher-directory: ./data/watchers
  tenant:
    auto-create-tenants: true
  source-cleanup:
    enabled: true
    database-path: ./data/watchers/source-cleanup.db
    max-active-jobs: 10000
    max-concurrent-actions: 2
  volumes:
    - id: primary
      mount-path: ./data/volume
      sharding-depth: 2
      buffer-size: 65536
      force-flush-after-write: true
```

Inject `StowRuntime`, `StoragePool`, or `TenantManager` into any application
service. The injected services are backed by the same runtime and use the same
tenant-scoped API as the framework-neutral example:

```java
@Service
public final class DocumentService {
    private final StoragePool storagePool;
    private final TenantManager tenantManager;

    public DocumentService(StoragePool storagePool, TenantManager tenantManager) {
        this.storagePool = storagePool;
        this.tenantManager = tenantManager;
    }

    public String store(String tenantId, byte[] bytes) {
        TenantContext tenant = tenantManager.get(tenantId);
        return storagePool.write(
                tenant,
                ContentSources.of(bytes),
                WriteOptions.ofOriginalFileName("document.bin"));
    }

    public byte[] load(String tenantId, String fileKey) throws IOException {
        TenantContext tenant = tenantManager.get(tenantId);
        try (InputStream input = storagePool.read(tenant, fileKey)) {
            return input.readAllBytes();
        }
    }
}
```

The starter owns runtime startup and shutdown; application code must not call
`start()` or `close()` on an injected runtime. For advanced integrations,
inject `StowRuntime` directly and use its service accessors. The starter does
not expose internal implementation classes. Do not call `Stow.open(...)` or
create another runtime in a Spring-managed service: that would bypass the
Spring singleton and its lifecycle management.

## Runnable Examples

The console sample is a dedicated write/read program. It writes and completes
one file, reads it immediately, closes the runtime, reopens the same data
directories, and reads the file again.

```bash
./mvnw -pl samples/stow-sample-console -am -DskipTests install
./mvnw -pl samples/stow-sample-console exec:java \
  -Dexec.mainClass=io.github.cocosip.stow.sample.ConsoleSampleApplication \
  -Dexec.args=sample-data
```

Windows:

```powershell
.\mvnw.cmd -pl samples/stow-sample-console -am -DskipTests install
.\mvnw.cmd -pl samples/stow-sample-console exec:java `
  "-Dexec.mainClass=io.github.cocosip.stow.sample.ConsoleSampleApplication" `
  "-Dexec.args=sample-data"
```

The Spring Boot sample uses the starter and can be started with
`spring-boot:run` after the reactor has been installed:

```bash
./mvnw -pl samples/stow-sample-spring-boot -am -DskipTests install
cd samples/stow-sample-spring-boot
../../mvnw spring-boot:run
```

Windows PowerShell:

```powershell
.\mvnw.cmd -pl samples/stow-sample-spring-boot -am -DskipTests install
Push-Location samples/stow-sample-spring-boot
..\..\mvnw.cmd spring-boot:run
Pop-Location
```

The sample reads its Stow paths from `src/main/resources/application.yml`.
The Maven run configuration sets the module directory as the working directory,
so the relative `sample-data/` paths stay inside this sample module.

The samples exercise write, claim, complete, close, reopen, and read behavior
when run manually in a module-local data directory. Sample modules intentionally
do not contain unit tests; automated coverage belongs to the core and starter
modules.

## Benchmark Baseline

Run the JMH runner with:

```bash
./mvnw -Pbenchmarks -pl benchmarks -am package
java -jar benchmarks/target/stow-benchmarks-0.1.0-SNAPSHOT-runner.jar \
  'io.github.cocosip.stow.benchmarks.StoragePoolBenchmark.*' \
  -wi 1 -i 2 -f 1 -rff benchmarks/results/storage-pool.json -rf json
```

The following baseline was measured on 2026-09-19 on Windows 11 x64 with
OpenJDK 21.0.12.1, one thread, one fork, a 1 KiB payload, one warmup
iteration, and two measurement iterations:

| Benchmark | Throughput |
| --- | ---: |
| `StoragePoolBenchmark.read` | 139.735 ops/s |
| `StoragePoolBenchmark.write` | 0.922 ops/s |
| `StoragePoolBenchmark.writeClaimComplete` | 0.493 ops/s |

These values are an execution record for this machine and configuration, not
a portability or performance guarantee. Compare future runs only when the
JDK, filesystem, payload, JMH parameters, and host conditions are recorded as
well. Generated JSON results stay under the ignored `benchmarks/results/`
directory.

## Build and Test

Run the complete local verification build with:

```bash
./mvnw verify
./mvnw -Pslf4j1-compat verify
./mvnw -Pspring-boot-compat verify
./mvnw -Pbenchmarks -pl benchmarks -am package
```

The compatibility profiles are build checks; normal applications should use
SLF4J 2.x. `spotless`, `spotbugs`, JaCoCo, unit tests, and Javadoc packaging
are part of the Maven lifecycle.

## GitHub Actions and Release

The `master` workflow runs on pushes and pull requests targeting `master`.
It tests Linux and Windows with JDK 21 and runs the Maven verification build.
It does not publish artifacts.

Version tags matching `v*` run the same verification on Linux and then deploy
the two release modules to Sonatype Central Portal. The tag `v1.0.0` publishes
version `1.0.0`; the leading `v` is removed before passing `revision` to
Maven. Samples and benchmarks have `maven.deploy.skip=true` and are never
published.

Configure these repository secrets before creating a release tag:

- `MAVEN_CENTRAL_USERNAME`: Central Portal user token username.
- `MAVEN_CENTRAL_TOKEN`: Central Portal user token password.
- `MAVEN_GPG_PRIVATE_KEY`: ASCII-armored signing key.
- `MAVEN_GPG_PASSPHRASE`: signing key passphrase.

The workflow and the release profile use the Maven server id `central`. No
credentials are stored in the repository.

## Modules

| Module | Purpose |
| --- | --- |
| `stow-core` | Public API, SPI, persistence, scheduling, recovery, and storage |
| `stow-spring-boot-starter` | Spring Boot configuration, lifecycle, Actuator, and metrics |
| `samples/stow-sample-console` | Framework-neutral runnable read/write example |
| `samples/stow-sample-spring-boot` | Spring Boot runnable integration example |
| `benchmarks` | JMH baseline runner; not a published artifact |

Only `io.github.cocosip:stow-core` and
`io.github.cocosip:stow-spring-boot-starter` are release artifacts.

## Documentation

- [Documentation index](docs/README.md)
- [API and configuration contract](docs/stow-api-contract.md)
- [Design](docs/stow-design.md)
- [Persistence and recovery contract](docs/stow-persistence-contract.md)
- [Operations and release guide](docs/operations-and-release.md)
