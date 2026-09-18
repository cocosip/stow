package io.github.cocosip.stow.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.exception.InvalidConfigurationException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StowConfigurationTest {

    @Test
    void providesEveryDocumentedDefault() {
        StowConfiguration configuration = StowConfiguration.builder().build();
        Path workingDirectory = Path.of("").toAbsolutePath().normalize();

        assertThat(configuration.paths().metadataDirectory()).isEqualTo(workingDirectory.resolve("stow-metadata"));
        assertThat(configuration.paths().quotaDirectory()).isEqualTo(workingDirectory.resolve("stow-quota"));
        assertThat(configuration.paths().queueDirectory()).isEqualTo(workingDirectory.resolve("stow-queue"));
        assertThat(configuration.paths().watcherDirectory()).isEqualTo(workingDirectory.resolve("stow-watchers"));

        assertThat(configuration.tenant().autoCreateTenants()).isFalse();
        assertThat(configuration.tenant().defaultQuota()).isZero();
        assertThat(configuration.tenant().preconfiguredTenants()).isEmpty();

        assertThat(configuration.metadata().backgroundPersistence()).isTrue();
        assertThat(configuration.metadata().maxQueueSize()).isEqualTo(100_000);
        assertThat(configuration.metadata().drainBatchSize()).isEqualTo(2_000);
        assertThat(configuration.metadata().softMergeThresholdPercent()).isEqualTo(90);
        assertThat(configuration.metadata().startupLoadBatchSize()).isEqualTo(2_000);
        assertThat(configuration.metadata().shutdownDrainTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(configuration.metadata().persistenceInterval()).isEqualTo(Duration.ofSeconds(2));

        assertThat(configuration.storage().completionGuardStripes()).isEqualTo(256);
        assertThat(configuration.storage().emptyQueueReclaimBatchSize()).isEqualTo(32);
        assertThat(configuration.storage().backgroundReclaimBatchSize()).isEqualTo(8);
        assertThat(configuration.storage().reclaimCooldown()).isEqualTo(Duration.ofSeconds(30));
        assertThat(configuration.storage().backgroundReclaimEnabled()).isTrue();

        assertThat(configuration.sqlite().journalMode()).isEqualTo(SqliteJournalMode.WAL);
        assertThat(configuration.sqlite().synchronousMode()).isEqualTo(SqliteSynchronousMode.NORMAL);
        assertThat(configuration.sqlite().cacheSizeKb()).isEqualTo(-4_000);
        assertThat(configuration.sqlite().busyTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(configuration.sqlite().checkpointAfterBatch()).isFalse();

        assertThat(configuration.retry().maxRetryCount()).isEqualTo(3);
        assertThat(configuration.retry().initialDelay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(configuration.retry().exponentialBackoff()).isTrue();
        assertThat(configuration.retry().maxDelay()).isEqualTo(Duration.ofMinutes(5));

        assertThat(configuration.journal().enabled()).isTrue();
        assertThat(configuration.journal().projectionEnabled()).isTrue();
        assertThat(configuration.journal().format()).isEqualTo(JournalFormat.BINARY_V1);
        assertThat(configuration.journal().ackMode()).isEqualTo(JournalAckMode.DURABLE);
        assertThat(configuration.journal().stateFlushDebounce()).isEqualTo(Duration.ofSeconds(1));
        assertThat(configuration.journal().linger()).isEqualTo(Duration.ofMillis(1));
        assertThat(configuration.journal().maxBatchRecords()).isEqualTo(16);
        assertThat(configuration.journal().maxBatchBytes()).isEqualTo(262_144);
        assertThat(configuration.journal().writerIdleTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(configuration.journal().asyncQueueCapacityPerTenant()).isEqualTo(8_192);
        assertThat(configuration.journal().balancedFlushWindow()).isEqualTo(Duration.ofMillis(5));

        assertThat(configuration.projection().maxRecordsPerTenantCycle()).isEqualTo(64);
        assertThat(configuration.projection().maxTenantsPerCycle()).isEqualTo(8);
        assertThat(configuration.projection().busyCycleDelay()).isEqualTo(Duration.ofMillis(500));
        assertThat(configuration.projection().idleCycleDelay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(configuration.projection().cycleTimeBudget()).isEqualTo(Duration.ofSeconds(2));

        assertThat(configuration.snapshot().enabled()).isTrue();
        assertThat(configuration.snapshot().interval()).isEqualTo(Duration.ofMinutes(15));
        assertThat(configuration.snapshot().minimumProgressBytes()).isEqualTo(1_048_576);

        assertThat(configuration.compaction().enabled()).isTrue();
        assertThat(configuration.compaction().minimumProcessedBytes()).isEqualTo(4_194_304);

        assertThat(configuration.cleanup().enabled()).isTrue();
        assertThat(configuration.cleanup().interval()).isEqualTo(Duration.ofHours(1));
        assertThat(configuration.cleanup().initialDelay()).isEqualTo(Duration.ofMinutes(1));
        assertThat(configuration.cleanup().processingTimeout()).isEqualTo(Duration.ofMinutes(30));
        assertThat(configuration.cleanup().completedRetention()).isZero();
        assertThat(configuration.cleanup().failedRetention()).isEqualTo(Duration.ofDays(3));
        assertThat(configuration.cleanup().permanentlyFailedDisposition())
                .isEqualTo(PermanentlyFailedDisposition.MOVE_TO_DEAD_LETTER);
        assertThat(configuration.cleanup().batchSizePerTenant()).isEqualTo(500);

        assertThat(configuration.orphanRecovery().enabled()).isFalse();
        assertThat(configuration.orphanRecovery().runOnStartup()).isFalse();
        assertThat(configuration.orphanRecovery().interval()).isEqualTo(Duration.ofHours(6));

        assertThat(configuration.statistics().enabled()).isFalse();
        assertThat(configuration.statistics().windowSize()).isEqualTo(Duration.ofMinutes(5));
        assertThat(configuration.statistics().retention()).isEqualTo(Duration.ofHours(1));
        assertThat(configuration.statistics().maxSeries()).isEqualTo(16_384);

        assertThat(configuration.volumes()).isEmpty();
        assertThat(configuration.watchers()).isEmpty();
    }

    @Test
    void normalizesConfiguredPathsAndDefensivelyCopiesCollections() {
        Path relativeMount = Path.of("data", "volume-1");
        var volumes = new java.util.ArrayList<>(
                java.util.List.of(new VolumeConfiguration("volume-1", relativeMount, 2, 65_536, true)));

        StowConfiguration.Builder builder = StowConfiguration.builder()
                .metadataDirectory(Path.of("data", "metadata"))
                .quotaDirectory(Path.of("data", "quota"))
                .queueDirectory(Path.of("data", "queue"))
                .watcherDirectory(Path.of("data", "watchers"))
                .volumes(volumes);
        volumes.clear();
        StowConfiguration configuration = builder.build();

        assertThat(configuration.paths().metadataDirectory()).isAbsolute().isNormalized();
        assertThat(configuration.volumes()).hasSize(1).isUnmodifiable();
        assertThat(configuration.volumes().getFirst().mountPath()).isAbsolute().isNormalized();
    }

    @ParameterizedTest(name = "rejects invalid configuration: {0}")
    @MethodSource("invalidConfigurations")
    void rejectsInvalidNumbersDurationsRangesAndRequiredValues(
            String description, Consumer<StowConfiguration.Builder> mutation) {
        assertThatThrownBy(() -> {
                    StowConfiguration.Builder builder = StowConfiguration.builder();
                    mutation.accept(builder);
                    builder.build();
                })
                .as(description)
                .isInstanceOf(InvalidConfigurationException.class);
    }

    static Stream<Arguments> invalidConfigurations() {
        return Stream.of(
                invalid("negative default quota", builder -> builder.defaultQuota(-1)),
                invalid("zero metadata queue", builder -> builder.maxQueueSize(0)),
                invalid("zero metadata drain batch", builder -> builder.drainBatchSize(0)),
                invalid("soft merge below range", builder -> builder.softMergeThresholdPercent(0)),
                invalid("soft merge above range", builder -> builder.softMergeThresholdPercent(101)),
                invalid("zero startup batch", builder -> builder.startupLoadBatchSize(0)),
                invalid("zero shutdown timeout", builder -> builder.shutdownDrainTimeout(Duration.ZERO)),
                invalid("negative persistence interval", builder -> builder.persistenceInterval(Duration.ofMillis(-1))),
                invalid("non power-of-two stripes", builder -> builder.completionGuardStripes(3)),
                invalid("zero empty reclaim batch", builder -> builder.emptyQueueReclaimBatchSize(0)),
                invalid("zero background reclaim batch", builder -> builder.backgroundReclaimBatchSize(0)),
                invalid("negative reclaim cooldown", builder -> builder.reclaimCooldown(Duration.ofSeconds(-1))),
                invalid("zero sqlite cache", builder -> builder.sqliteCacheSizeKb(0)),
                invalid("zero sqlite busy timeout", builder -> builder.sqliteBusyTimeout(Duration.ZERO)),
                invalid("negative retries", builder -> builder.maxRetryCount(-1)),
                invalid("negative retry delay", builder -> builder.retryInitialDelay(Duration.ofSeconds(-1))),
                invalid("retry cap below initial", builder -> builder.retryMaxDelay(Duration.ofSeconds(1))),
                invalid(
                        "negative journal debounce",
                        builder -> builder.journalStateFlushDebounce(Duration.ofMillis(-1))),
                invalid("negative journal linger", builder -> builder.journalLinger(Duration.ofMillis(-1))),
                invalid("zero journal record batch", builder -> builder.journalMaxBatchRecords(0)),
                invalid("zero journal byte batch", builder -> builder.journalMaxBatchBytes(0)),
                invalid("zero journal idle timeout", builder -> builder.journalWriterIdleTimeout(Duration.ZERO)),
                invalid("zero async capacity", builder -> builder.journalAsyncQueueCapacityPerTenant(0)),
                invalid(
                        "negative balanced window",
                        builder -> builder.journalBalancedFlushWindow(Duration.ofMillis(-1))),
                invalid("zero projection records", builder -> builder.projectionMaxRecordsPerTenantCycle(0)),
                invalid("zero projection tenants", builder -> builder.projectionMaxTenantsPerCycle(0)),
                invalid("negative busy delay", builder -> builder.projectionBusyCycleDelay(Duration.ofMillis(-1))),
                invalid("negative idle delay", builder -> builder.projectionIdleCycleDelay(Duration.ofMillis(-1))),
                invalid("zero cycle budget", builder -> builder.projectionCycleTimeBudget(Duration.ZERO)),
                invalid("zero snapshot interval", builder -> builder.snapshotInterval(Duration.ZERO)),
                invalid("zero snapshot progress", builder -> builder.snapshotMinimumProgressBytes(0)),
                invalid("zero compaction progress", builder -> builder.compactionMinimumProcessedBytes(0)),
                invalid("zero cleanup interval", builder -> builder.cleanupInterval(Duration.ZERO)),
                invalid("negative cleanup delay", builder -> builder.cleanupInitialDelay(Duration.ofMillis(-1))),
                invalid("zero processing timeout", builder -> builder.processingTimeout(Duration.ZERO)),
                invalid("negative completed retention", builder -> builder.completedRetention(Duration.ofMillis(-1))),
                invalid("negative failed retention", builder -> builder.failedRetention(Duration.ofMillis(-1))),
                invalid("zero cleanup batch", builder -> builder.cleanupBatchSizePerTenant(0)),
                invalid("zero orphan interval", builder -> builder.orphanRecoveryInterval(Duration.ZERO)),
                invalid("zero statistics window", builder -> builder.statisticsWindowSize(Duration.ZERO)),
                invalid("retention below window", builder -> builder.statisticsRetention(Duration.ofMinutes(1))),
                invalid("zero max series", builder -> builder.statisticsMaxSeries(0)));
    }

    @ParameterizedTest(name = "rejects invalid path layout: {0}")
    @MethodSource("invalidPaths")
    void rejectsNullOverlappingAndDuplicatePaths(String description, Consumer<StowConfiguration.Builder> mutation) {
        assertThatThrownBy(() -> {
                    StowConfiguration.Builder builder = StowConfiguration.builder();
                    mutation.accept(builder);
                    builder.build();
                })
                .as(description)
                .isInstanceOf(InvalidConfigurationException.class);
    }

    static Stream<Arguments> invalidPaths() {
        return Stream.of(
                invalid("null metadata path", builder -> builder.metadataDirectory(null)),
                invalid("duplicate roots", builder -> builder.quotaDirectory(Path.of("stow-metadata"))),
                invalid("nested roots", builder -> builder.quotaDirectory(Path.of("stow-metadata", "quota"))));
    }

    private static Arguments invalid(String description, Consumer<StowConfiguration.Builder> mutation) {
        return Arguments.of(description, mutation);
    }
}
