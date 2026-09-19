package io.github.cocosip.stow.internal.cleanup;

import io.github.cocosip.stow.model.CleanupStatistics;
import io.github.cocosip.stow.spi.StorageVolume;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public final class JunkFileCleaner {

    private static final Pattern TEMPORARY = Pattern.compile(
            "^\\.[0-9a-f]{32}(?:\\.[A-Za-z0-9._-]{1,31})?\\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.tmp$");
    private static final Pattern DATABASE_BACKUP =
            Pattern.compile("^(?:metadata|quotas)\\.db\\.corrupt\\.[0-9]+\\.bak$");
    private final List<StorageVolume> volumes;
    private final List<Path> databaseRoots;
    private final Clock clock;

    public JunkFileCleaner(List<StorageVolume> volumes, List<Path> databaseRoots, Clock clock) {
        this.volumes = List.copyOf(Objects.requireNonNull(volumes, "volumes"));
        this.databaseRoots = List.copyOf(Objects.requireNonNull(databaseRoots, "databaseRoots"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public CleanupStatistics cleanupJunkFiles(int maxFiles) {
        if (maxFiles <= 0) throw new IllegalArgumentException("maxFiles must be positive");
        CleanupStatisticsBuilder statistics = new CleanupStatisticsBuilder(clock);
        for (StorageVolume volume : volumes) {
            try (var paths = Files.walk(volume.mountPath())) {
                paths.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                        .filter(path ->
                                TEMPORARY.matcher(path.getFileName().toString()).matches())
                        .limit(maxFiles)
                        .forEach(path -> delete(path, volume.mountPath(), statistics));
            } catch (IOException exception) {
                statistics.error(
                        null, "junk-scan", exception.getMessage() == null ? "scan failed" : exception.getMessage());
            }
        }
        return statistics.build();
    }

    public CleanupStatistics cleanupInvalidDatabaseBackups(int maxFiles) {
        if (maxFiles <= 0) throw new IllegalArgumentException("maxFiles must be positive");
        CleanupStatisticsBuilder statistics = new CleanupStatisticsBuilder(clock);
        for (Path root : databaseRoots) {
            if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) continue;
            try (var paths = Files.walk(root)) {
                paths.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                        .filter(path -> DATABASE_BACKUP
                                .matcher(path.getFileName().toString())
                                .matches())
                        .limit(maxFiles)
                        .forEach(path -> delete(path, root, statistics));
            } catch (IOException exception) {
                statistics.error(
                        null,
                        "database-backup-scan",
                        exception.getMessage() == null ? "scan failed" : exception.getMessage());
            }
        }
        return statistics.build();
    }

    public CleanupStatistics cleanupEmptyDirectories() {
        CleanupStatisticsBuilder statistics = new CleanupStatisticsBuilder(clock);
        for (StorageVolume volume : volumes) {
            Path root = volume.mountPath().toAbsolutePath().normalize();
            if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) continue;
            try (var paths = Files.walk(root)) {
                paths.filter(path -> !path.equals(root))
                        .filter(path -> Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS))
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try (var children = Files.list(path)) {
                                if (children.findAny().isEmpty()) {
                                    Files.deleteIfExists(path);
                                    statistics.succeeded(tenantOf(root, path), 0);
                                }
                            } catch (IOException exception) {
                                statistics.error(
                                        tenantOf(root, path),
                                        "empty-directory",
                                        exception.getMessage() == null ? "delete failed" : exception.getMessage());
                            }
                        });
            } catch (IOException exception) {
                statistics.error(
                        null,
                        "empty-directory-scan",
                        exception.getMessage() == null ? "scan failed" : exception.getMessage());
            }
        }
        return statistics.build();
    }

    private static void delete(Path path, Path root, CleanupStatisticsBuilder statistics) {
        statistics.scanned();
        try {
            long bytes = Files.size(path);
            Files.deleteIfExists(path);
            statistics.succeeded(tenantOf(root, path), bytes);
        } catch (IOException exception) {
            statistics.failed(tenantOf(root, path), "delete", exception);
        }
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
