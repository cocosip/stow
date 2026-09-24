package io.github.cocosip.stow.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.exception.InvalidConfigurationException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class PublicModelTest {

    private static final String FILE_KEY = "0123456789abcdef0123456789abcdef";
    private static final Instant NOW = Instant.parse("2026-09-18T00:00:00Z");

    @Test
    void makesDefensiveCopiesOfPublicCollections() {
        List<String> globs = new ArrayList<>(List.of("**/*.dcm"));
        WatcherConfiguration watcher = new WatcherConfiguration(
                "watcher-1",
                "tenant-1",
                WatcherTenantMode.SINGLE_TENANT,
                false,
                Path.of("inbox"),
                true,
                true,
                globs,
                PostImportAction.DELETE,
                null,
                Duration.ofSeconds(5),
                1_000_000,
                Duration.ofSeconds(1),
                Duration.ofMillis(250),
                2,
                4,
                Duration.ofDays(7),
                Duration.ofSeconds(1));
        globs.clear();

        Map<String, ComponentHealth> components = new HashMap<>();
        components.put("runtime", new ComponentHealth(HealthStatus.UP, "running", NOW));
        RuntimeHealth health = new RuntimeHealth(HealthStatus.UP, components);
        components.clear();

        List<MaintenanceError> errors = new ArrayList<>(List.of(new MaintenanceError("tenant-1", "cleanup", "failed")));
        CleanupStatistics statistics = new CleanupStatistics(NOW, NOW.plusSeconds(1), 1, 0, 0, 1, 0, 1, errors);
        errors.clear();

        assertThat(watcher.globs()).containsExactly("**/*.dcm").isUnmodifiable();
        assertThat(health.components()).containsOnlyKeys("runtime").isUnmodifiable();
        assertThat(statistics.errors()).hasSize(1).isUnmodifiable();
    }

    @Test
    void keepsLegacyWatcherConstructorsAndDefaultsNewCleanupFields() {
        WatcherConfiguration watcher = new WatcherConfiguration(
                "watcher-1",
                "tenant-1",
                WatcherTenantMode.SINGLE_TENANT,
                false,
                Path.of("inbox"),
                true,
                true,
                List.of("**/*.dcm"),
                PostImportAction.DELETE,
                null,
                Duration.ofSeconds(5),
                1_000_000,
                Duration.ofSeconds(1),
                Duration.ofMillis(250),
                2,
                4,
                Duration.ofDays(7),
                Duration.ofSeconds(1));
        WatcherRootConfiguration root = new WatcherRootConfiguration(
                Path.of("inbox"), true, false, true, List.of("**/*.dcm"), PostImportAction.DELETE, null);
        WatcherScanResult result = new WatcherScanResult("watcher-1", NOW, NOW, 1, 1, 0, 0, 42, List.of());

        assertThat(watcher.sourceCleanupFailureDirectory())
                .isEqualTo(Path.of("stow-source-failed").toAbsolutePath().normalize());
        assertThat(watcher.maxPostImportActionAttempts()).isEqualTo(5);
        assertThat(watcher.postImportRetryInitialDelay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(watcher.postImportRetryMaxDelay()).isEqualTo(Duration.ofMinutes(5));
        assertThat(root.sourceCleanupFailureDirectory())
                .isEqualTo(Path.of("stow-source-failed").toAbsolutePath().normalize());
        assertThat(result.postImportActionsRetried()).isZero();
        assertThat(result.filesQuarantined()).isZero();
        assertThat(result.importsDeferred()).isZero();
    }

    @ParameterizedTest(name = "validates required field: {0}")
    @MethodSource("invalidRequiredFields")
    void validatesRequiredRecordFields(String description, Runnable constructorCall) {
        assertThatThrownBy(constructorCall::run).as(description).isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Arguments> invalidRequiredFields() {
        UUID leaseId = UUID.randomUUID();
        FileLocation location = location("", "name.dcm", ".dcm");
        return Stream.of(
                Arguments.of(
                        "processing lease tenant", (Runnable) () -> new ProcessingLease(null, FILE_KEY, leaseId, NOW)),
                Arguments.of("processing lease file key", (Runnable)
                        () -> new ProcessingLease("tenant-1", "bad", leaseId, NOW)),
                Arguments.of("claimed file location", (Runnable)
                        () -> new ClaimedFile(null, new ProcessingLease("tenant-1", FILE_KEY, leaseId, NOW))),
                Arguments.of("tenant id", (Runnable) () -> new TenantContext("..", TenantStatus.ENABLED, NOW, NOW)),
                Arguments.of(
                        "directory quota count", (Runnable) () -> new DirectoryQuota("tenant-1", "/", -1, 0, true)),
                Arguments.of("component health status", (Runnable) () -> new ComponentHealth(null, "summary", NOW)),
                Arguments.of("queue event sequence", (Runnable) () -> event(0, "", "name.dcm", ".dcm")),
                Arguments.of("stored info status", (Runnable)
                        () -> new StoredFileInfo(FILE_KEY, "tenant-1", 1, NOW, null, 0, "name.dcm", ".dcm")),
                Arguments.of("file location physical path", (Runnable) () -> new FileLocation(
                        location.fileKey(),
                        location.tenantId(),
                        location.volumeId(),
                        null,
                        location.logicalDirectory(),
                        location.fileSize(),
                        location.createdAt(),
                        location.status(),
                        location.retryCount(),
                        location.lastFailedAt(),
                        location.lastError(),
                        location.availableAt())));
    }

    @Test
    void truncatesPersistedErrorSummariesTo4096Characters() {
        String oversized = "x".repeat(5_000);

        FileLocation location = location(oversized, "name.dcm", ".dcm");
        QueueEventRecord event = event(1, oversized, "name.dcm", ".dcm");

        assertThat(location.lastError()).hasSize(4_096);
        assertThat(event.errorMessage()).hasSize(4_096);
    }

    @Test
    void canonicalizesLogicalDirectoryToASingleQuotaKey() {
        // "docs", "/docs", "docs/", and "docs//." must all map to one directory-quota key
        assertThat(new WriteOptions(null, "docs").logicalDirectory()).isEqualTo("/docs");
        assertThat(new WriteOptions(null, "/docs").logicalDirectory()).isEqualTo("/docs");
        assertThat(new WriteOptions(null, "docs/").logicalDirectory()).isEqualTo("/docs");
        assertThat(new WriteOptions(null, "docs//./sub").logicalDirectory())
                .isEqualTo(new WriteOptions(null, "/docs/sub").logicalDirectory());
        assertThat(new DirectoryQuota("tenant-1", "docs///", 0, 10, true).logicalDirectory())
                .isEqualTo("/docs");
        assertThat(new WriteOptions(null, " ").logicalDirectory()).isEqualTo("/");
        assertThatThrownBy(() -> new WriteOptions(null, "docs/../secret")).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "enforces public string limit: {0}")
    @MethodSource("oversizedNamesAndExtensions")
    void enforcesFileNameAndExtensionLimits(String description, Runnable constructorCall) {
        assertThatThrownBy(constructorCall::run).as(description).isInstanceOf(IllegalArgumentException.class);
    }

    static Stream<Arguments> oversizedNamesAndExtensions() {
        return Stream.of(
                Arguments.of("write option filename", (Runnable) () -> new WriteOptions("n".repeat(256), "/")),
                Arguments.of("stored filename", (Runnable) () -> new StoredFileInfo(
                        FILE_KEY, "tenant-1", 1, NOW, FileProcessingStatus.PENDING, 0, "n".repeat(256), ".dcm")),
                Arguments.of("stored extension", (Runnable) () -> new StoredFileInfo(
                        FILE_KEY,
                        "tenant-1",
                        1,
                        NOW,
                        FileProcessingStatus.PENDING,
                        0,
                        "name.dcm",
                        "." + "x".repeat(32))),
                Arguments.of("event filename", (Runnable) () -> event(1, "", "n".repeat(256), ".dcm")),
                Arguments.of("event extension", (Runnable) () -> event(1, "", "name.dcm", "." + "x".repeat(32))));
    }

    @Test
    void exposesStableExceptionCode() {
        InvalidConfigurationException exception = new InvalidConfigurationException("invalid");

        assertThat(exception.errorCode()).isEqualTo("STOW_INVALID_CONFIGURATION");
        assertThat(exception).isInstanceOf(RuntimeException.class);
    }

    private static FileLocation location(String error, String originalFileName, String extension) {
        return new FileLocation(
                FILE_KEY,
                "tenant-1",
                "volume-1",
                Path.of("storage", FILE_KEY + ".dcm"),
                "/",
                42,
                NOW,
                FileProcessingStatus.FAILED,
                1,
                NOW,
                error,
                NOW.plusSeconds(1));
    }

    private static QueueEventRecord event(
            long sequenceNumber, String error, String originalFileName, String extension) {
        return new QueueEventRecord(
                1,
                UUID.randomUUID(),
                "tenant-1",
                FILE_KEY,
                QueueEventType.PROCESSING_FAILED,
                NOW,
                sequenceNumber,
                "volume-1",
                Path.of("storage", FILE_KEY + extension),
                "/",
                42,
                FileProcessingStatus.FAILED,
                UUID.randomUUID(),
                NOW,
                1,
                NOW.plusSeconds(1),
                error,
                originalFileName,
                extension);
    }
}
