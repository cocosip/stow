package io.github.cocosip.stow.internal.filesystem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.exception.InsufficientStorageException;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PowerOfTwoVolumeSelectorTest {

    @Test
    void excludesUnhealthyAndFullVolumesAfterConsecutiveHealthChecks() {
        TestVolume unhealthy = new TestVolume("unhealthy", false, 1_000);
        TestVolume full = new TestVolume("full", true, 0);
        TestVolume writable = new TestVolume("writable", true, 500);
        VolumeRegistry registry = new VolumeRegistry(List.of(unhealthy, full, writable));
        PowerOfTwoVolumeSelector selector = new PowerOfTwoVolumeSelector(registry, new Random(7));

        assertThat(selector.orderedWriteCandidates(100))
                .extracting(StorageVolume::id)
                .containsExactly("writable");
        assertThat(unhealthy.healthChecks).isGreaterThanOrEqualTo(2);
        assertThat(full.healthChecks).isGreaterThanOrEqualTo(2);
        assertThat(writable.healthChecks).isGreaterThanOrEqualTo(2);
    }

    @Test
    void distributesPrimaryChoicesAndOrdersTheSampleByAvailableCapacity() {
        List<StorageVolume> volumes = List.of(
                new TestVolume("a", true, 100),
                new TestVolume("b", true, 200),
                new TestVolume("c", true, 300),
                new TestVolume("d", true, 400));
        PowerOfTwoVolumeSelector selector = new PowerOfTwoVolumeSelector(new VolumeRegistry(volumes), new Random(11));
        var selectedIds = new LinkedHashSet<String>();

        for (int attempt = 0; attempt < 200; attempt++) {
            List<StorageVolume> candidates = selector.orderedWriteCandidates(1);
            selectedIds.add(candidates.getFirst().id());
            assertThat(candidates).containsExactlyInAnyOrderElementsOf(volumes);
            assertThat(candidates.getFirst().availableCapacity())
                    .isGreaterThanOrEqualTo(candidates.get(1).availableCapacity());
        }

        assertThat(selectedIds).containsExactlyInAnyOrder("b", "c", "d");
    }

    @Test
    void registryClosesEveryVolume() {
        TestVolume first = new TestVolume("first", true, 100);
        TestVolume second = new TestVolume("second", true, 100);

        new VolumeRegistry(List.of(first, second)).close();

        assertThat(first.closed).isTrue();
        assertThat(second.closed).isTrue();
    }

    @Test
    void closesCreatedVolumesWhenConfigurationContainsDuplicateIds(@TempDir Path temporaryDirectory) {
        TestVolume first = new TestVolume("duplicate", true, 100);
        TestVolume second = new TestVolume("duplicate", true, 100);
        var nextVolume = new AtomicInteger();
        List<VolumeConfiguration> configurations = List.of(
                configuration("first", temporaryDirectory.resolve("first")),
                configuration("second", temporaryDirectory.resolve("second")));

        assertThatThrownBy(() -> new VolumeRegistry(
                        configurations, ignored -> nextVolume.getAndIncrement() == 0 ? first : second))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate storage volume id");
        assertThat(first.closed).isTrue();
        assertThat(second.closed).isTrue();
    }

    @Test
    void rejectsSelectionWhenNoVolumeHasEnoughCapacity() {
        PowerOfTwoVolumeSelector selector = new PowerOfTwoVolumeSelector(
                new VolumeRegistry(List.of(new TestVolume("full", true, 0))), new Random(3));

        assertThatThrownBy(() -> selector.orderedWriteCandidates(1)).isInstanceOf(InsufficientStorageException.class);
    }

    private static VolumeConfiguration configuration(String id, Path mountPath) {
        return new VolumeConfiguration(id, mountPath, 0, 1_024, false);
    }

    private static final class TestVolume implements StorageVolume {

        private final String id;
        private final boolean healthy;
        private final long availableCapacity;
        private int healthChecks;
        private boolean closed;

        private TestVolume(String id, boolean healthy, long availableCapacity) {
            this.id = id;
            this.healthy = healthy;
            this.availableCapacity = availableCapacity;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public Path mountPath() {
            return Path.of(id);
        }

        @Override
        public boolean healthy() {
            healthChecks++;
            return healthy;
        }

        @Override
        public long totalCapacity() {
            return availableCapacity;
        }

        @Override
        public long availableCapacity() {
            return availableCapacity;
        }

        @Override
        public Path buildPath(String tenantId, String fileKey, String extension) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long write(Path target, InputStream content) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream read(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(Path path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void move(Path source, Path target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
