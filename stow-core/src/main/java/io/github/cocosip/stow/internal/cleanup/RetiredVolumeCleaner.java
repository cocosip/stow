package io.github.cocosip.stow.internal.cleanup;

import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Applies the physical-file policy for volumes that are no longer writable. */
public final class RetiredVolumeCleaner {

    private final List<StorageVolume> volumes;
    private final Clock clock;

    public RetiredVolumeCleaner(List<StorageVolume> volumes, Clock clock) {
        this.volumes = List.copyOf(Objects.requireNonNull(volumes, "volumes"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CleanupStatistics clean(Set<String> retiredVolumeIds, boolean purgePhysicalFiles, int maxFiles) {
        Objects.requireNonNull(retiredVolumeIds, "retiredVolumeIds");
        if (maxFiles <= 0) throw new IllegalArgumentException("maxFiles must be positive");
        CleanupStatisticsBuilder statistics = new CleanupStatisticsBuilder(clock);
        for (StorageVolume volume : volumes) {
            if (!retiredVolumeIds.contains(volume.id())) continue;
            try (var paths = Files.walk(volume.mountPath())) {
                paths.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                        .limit(maxFiles)
                        .forEach(path -> {
                            statistics.scanned();
                            if (!purgePhysicalFiles) {
                                statistics.skipped();
                                return;
                            }
                            try {
                                long size = Files.size(path);
                                Files.deleteIfExists(path);
                                statistics.succeeded(tenantOf(volume.mountPath(), path), size);
                            } catch (IOException exception) {
                                statistics.failed(tenantOf(volume.mountPath(), path), "retired-volume", exception);
                            }
                        });
            } catch (IOException exception) {
                statistics.error(
                        null,
                        "retired-volume-scan",
                        exception.getMessage() == null ? "scan failed" : exception.getMessage());
            }
        }
        return statistics.build();
    }

    private static String tenantOf(Path root, Path path) {
        try {
            Path relative = root.toAbsolutePath()
                    .normalize()
                    .relativize(path.toAbsolutePath().normalize());
            return relative.getNameCount() == 0 ? null : relative.getName(0).toString();
        } catch (RuntimeException exception) {
            return null;
        }
    }
}
