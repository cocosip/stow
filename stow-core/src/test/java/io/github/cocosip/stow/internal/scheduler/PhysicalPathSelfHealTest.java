package io.github.cocosip.stow.internal.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.api.ContentSources;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.exception.StoredFileNotFoundException;
import io.github.cocosip.stow.internal.journal.BinaryV1JournalCodec;
import io.github.cocosip.stow.internal.journal.FileQueueEventJournal;
import io.github.cocosip.stow.internal.projection.ProjectionCursorStore;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import io.github.cocosip.stow.model.WriteOptions;
import io.github.cocosip.stow.spi.QueueEventJournal;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class PhysicalPathSelfHealTest {

    @Test
    void readRepairsStalePhysicalPathFromCanonicalVolumePath() throws Exception {
        Fixture fixture = new Fixture();
        String fileKey =
                fixture.pool.write(fixture.tenant, ContentSources.of("hello".getBytes()), WriteOptions.defaults());
        String canonical =
                fixture.metadata.find("tenant-a", fileKey).orElseThrow().physicalPath();
        assertThat(canonical)
                .isEqualTo(fixture.volume.canonicalPath("tenant-a", fileKey, "").toString());

        // Simulate metadata drift after a volume repair: the row points at a path that
        // no longer exists while the canonical volume path still holds the file.
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + fixture.metadataDatabase);
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("UPDATE files SET physical_path='" + canonical + ".stale'");
        }

        try (InputStream input = fixture.pool.read(fixture.tenant, fileKey)) {
            assertThat(input.readAllBytes()).containsExactly("hello".getBytes());
        }
        assertThat(fixture.metadata.find("tenant-a", fileKey).orElseThrow().physicalPath())
                .isEqualTo(canonical);
    }

    private static final class Fixture {
        private final Path root = Files.createTempDirectory(Path.of("target"), "self-heal-");
        private final Clock clock = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
        private final TenantContext tenant =
                new TenantContext("tenant-a", TenantStatus.ENABLED, clock.instant(), clock.instant());
        private final SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        private final SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        private final QueueEventJournal journal =
                new FileQueueEventJournal(root.resolve("journal"), journalConfiguration(), new BinaryV1JournalCodec());
        private final SelfHealVolume volume = new SelfHealVolume("volume-a", root.resolve("volume"));
        private final StoragePool pool;
        private final Path metadataDatabase =
                root.resolve("metadata").resolve("tenant-a").resolve("metadata.db");

        private Fixture() throws Exception {
            QueueProjectionService projection = new QueueProjectionService(
                    journal,
                    new QueueEventReducer(metadata, quota),
                    new ProjectionCursorStore(root.resolve("cursor"), clock));
            pool = new DefaultStoragePool(id -> tenant, quota, metadata, projection, journal, volume, clock);
        }

        private static io.github.cocosip.stow.config.JournalConfiguration journalConfiguration() {
            return new io.github.cocosip.stow.config.JournalConfiguration(
                    true,
                    true,
                    io.github.cocosip.stow.config.JournalFormat.BINARY_V1,
                    io.github.cocosip.stow.config.JournalAckMode.DURABLE,
                    Duration.ZERO,
                    Duration.ZERO,
                    16,
                    262_144,
                    Duration.ofSeconds(30),
                    16,
                    Duration.ZERO);
        }
    }

    private static final class SelfHealVolume implements StorageVolume {
        private final String id;
        private final Path root;
        private final Map<Path, byte[]> files = new ConcurrentHashMap<>();

        private SelfHealVolume(String id, Path root) throws IOException {
            this.id = id;
            this.root = root;
            Files.createDirectories(root);
        }

        private Path canonicalPath(String tenantId, String fileKey, String extension) {
            return root.resolve(tenantId).resolve(fileKey + extension);
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public Path mountPath() {
            return root;
        }

        @Override
        public boolean healthy() {
            return true;
        }

        @Override
        public long totalCapacity() {
            return 10_000;
        }

        @Override
        public long availableCapacity() {
            return 10_000;
        }

        @Override
        public Path buildPath(String tenantId, String fileKey, String extension) {
            return canonicalPath(tenantId, fileKey, extension);
        }

        @Override
        public long write(Path target, InputStream content) {
            try {
                Files.createDirectories(target.getParent());
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                content.transferTo(output);
                byte[] bytes = output.toByteArray();
                files.put(target, bytes);
                Files.write(target, bytes);
                return bytes.length;
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }

        @Override
        public InputStream read(Path path) {
            byte[] bytes = files.get(path);
            if (bytes == null) {
                throw new StoredFileNotFoundException("Stored file does not exist");
            }
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public void delete(Path path) {
            files.remove(path);
        }

        @Override
        public void move(Path source, Path target) {
            files.put(target, files.remove(source));
        }

        @Override
        public void close() {}
    }
}
