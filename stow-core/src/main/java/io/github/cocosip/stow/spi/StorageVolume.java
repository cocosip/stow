package io.github.cocosip.stow.spi;

import java.io.InputStream;
import java.nio.file.Path;

public interface StorageVolume extends AutoCloseable {

    @Override
    void close();

    String id();

    Path mountPath();

    boolean healthy();

    /**
     * Forces a synchronous health probe, bypassing any cached result. Called after a
     * failed write so candidate selection re-evaluates the volume on fresh evidence.
     */
    default boolean probeHealth() {
        return healthy();
    }

    long totalCapacity();

    long availableCapacity();

    Path buildPath(String tenantId, String fileKey, String extension);

    long write(Path target, InputStream content);

    InputStream read(Path path);

    void delete(Path path);

    void move(Path source, Path target);
}
