package io.github.cocosip.stow.internal.cleanup;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.QuotaReservation;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class OrphanedMetadataCleanerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-18T00:00:00Z"), ZoneOffset.UTC);
    private static final String TENANT = "tenant-a";
    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private static final String DIR = "/";

    @Test
    void keepsRowsWhosePhysicalFileExists() throws Exception {
        Fixture fixture = new Fixture();
        fixture.publish();

        CleanupStatistics statistics = fixture.cleaner.run();

        assertThat(statistics.succeededCount()).isZero();
        assertThat(fixture.metadata.find(TENANT, KEY)).isPresent();
        assertThat(fixture.quota.tenantCurrentCount(TENANT)).isEqualTo(1);
    }

    @Test
    void removesRowsWhosePhysicalFileVanishedAndReleasesQuota() throws Exception {
        Fixture fixture = new Fixture();
        fixture.publish();
        Files.delete(fixture.canonical);

        CleanupStatistics statistics = fixture.cleaner.run();

        assertThat(statistics.succeededCount()).isEqualTo(1);
        assertThat(fixture.metadata.find(TENANT, KEY)).isEmpty();
        assertThat(fixture.quota.tenantCurrentCount(TENANT)).isZero();
    }

    private static final class Fixture {
        private final Path root = Files.createTempDirectory(Path.of("target"), "orphan-metadata-");
        private final SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), CLOCK, ignored -> 10);
        private final SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), CLOCK);
        private final MapVolume volume = new MapVolume("volume-a", root.resolve("volume"));
        private final OrphanedMetadataCleaner cleaner =
                new OrphanedMetadataCleaner(metadata, quota, java.util.List.of(volume), CLOCK);
        private Path canonical;

        private Fixture() throws Exception {}

        private void publish() throws Exception {
            canonical = volume.buildPath(TENANT, KEY, ".bin");
            try (InputStream content = new ByteArrayInputStream(new byte[] {1, 2, 3})) {
                volume.write(canonical, content);
            }
            QuotaReservation reservation = quota.reserve(TENANT, KEY, DIR);
            QueueEventReducer reducer = new QueueEventReducer(metadata, quota);
            reducer.apply(new QueueEventRecord(
                    1,
                    UUID.randomUUID(),
                    TENANT,
                    KEY,
                    QueueEventType.ACCEPTED,
                    CLOCK.instant(),
                    1,
                    volume.id(),
                    canonical,
                    DIR,
                    3,
                    FileProcessingStatus.PENDING,
                    null,
                    null,
                    0,
                    null,
                    null,
                    "file.bin",
                    ".bin"));
            if (reservation == null) throw new IllegalStateException("reservation missing");
        }
    }

    private static final class MapVolume implements StorageVolume {
        private final String id;
        private final Path root;
        private final Map<Path, byte[]> files = new ConcurrentHashMap<>();

        private MapVolume(String id, Path root) throws IOException {
            this.id = id;
            this.root = root;
            Files.createDirectories(root);
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
            return root.resolve(tenantId).resolve(fileKey + extension);
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
                throw new io.github.cocosip.stow.exception.StoredFileNotFoundException("Stored file does not exist");
            }
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public void delete(Path path) {
            files.remove(path);
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
            }
        }

        @Override
        public void move(Path source, Path target) {
            try {
                Files.createDirectories(target.getParent());
                files.put(target, files.remove(source));
                Files.move(source, target);
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }

        @Override
        public void close() {}
    }
}
