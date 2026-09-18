package io.github.cocosip.stow.internal.filesystem;

import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.spi.StorageVolume;
import io.github.cocosip.stow.spi.StorageVolumeProvider;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

final class VolumeRegistry implements AutoCloseable {

    private static final int REQUIRED_HEALTHY_CHECKS = 2;

    private final List<StorageVolume> volumes;
    private final Map<String, StorageVolume> volumesById;
    private final Map<String, Integer> healthyChecks = new LinkedHashMap<>();

    VolumeRegistry(List<StorageVolume> volumes) {
        Objects.requireNonNull(volumes, "volumes");
        Map<String, StorageVolume> byId = new LinkedHashMap<>();
        for (StorageVolume volume : volumes) {
            StorageVolume current = Objects.requireNonNull(volume, "volume");
            if (byId.putIfAbsent(current.id(), current) != null) {
                throw new IllegalArgumentException("Duplicate storage volume id: " + current.id());
            }
            healthyChecks.put(current.id(), 0);
        }
        this.volumes = List.copyOf(byId.values());
        volumesById = Map.copyOf(byId);
        refreshHealth();
    }

    VolumeRegistry(List<VolumeConfiguration> configurations, StorageVolumeProvider provider) {
        this(createVolumes(configurations, provider));
    }

    List<StorageVolume> all() {
        return volumes;
    }

    Optional<StorageVolume> find(String volumeId) {
        return Optional.ofNullable(volumesById.get(volumeId));
    }

    synchronized List<StorageVolume> writableVolumes(long requiredBytes) {
        if (requiredBytes < 0) {
            throw new IllegalArgumentException("requiredBytes must not be negative");
        }
        refreshHealth();
        return volumes.stream()
                .filter(volume -> healthyChecks.get(volume.id()) >= REQUIRED_HEALTHY_CHECKS)
                .filter(volume -> hasCapacity(volume, requiredBytes))
                .toList();
    }

    @Override
    public void close() {
        Exception failure = null;
        for (StorageVolume volume : volumes) {
            try {
                volume.close();
            } catch (Exception exception) {
                if (failure == null) {
                    failure = exception;
                } else {
                    failure.addSuppressed(exception);
                }
            }
        }
        if (failure != null) {
            throw new IllegalStateException("Unable to close storage volumes", failure);
        }
    }

    private synchronized void refreshHealth() {
        for (StorageVolume volume : volumes) {
            try {
                int consecutive = volume.healthy() ? healthyChecks.get(volume.id()) + 1 : 0;
                healthyChecks.put(volume.id(), Math.min(consecutive, REQUIRED_HEALTHY_CHECKS));
            } catch (RuntimeException exception) {
                healthyChecks.put(volume.id(), 0);
            }
        }
    }

    private static boolean hasCapacity(StorageVolume volume, long requiredBytes) {
        try {
            long available = volume.availableCapacity();
            return available > 0 && available >= requiredBytes;
        } catch (RuntimeException exception) {
            return false;
        }
    }

    private static List<StorageVolume> createVolumes(
            List<VolumeConfiguration> configurations, StorageVolumeProvider provider) {
        Objects.requireNonNull(configurations, "configurations");
        Objects.requireNonNull(provider, "provider");
        List<StorageVolume> created = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        try {
            for (VolumeConfiguration configuration : configurations) {
                StorageVolume volume = Objects.requireNonNull(
                        provider.create(Objects.requireNonNull(configuration, "configuration")), "created volume");
                created.add(volume);
                if (!ids.add(volume.id())) {
                    throw new IllegalArgumentException("Duplicate storage volume id: " + volume.id());
                }
            }
            return created;
        } catch (RuntimeException exception) {
            closeAfterFailedCreation(created, exception);
            throw exception;
        }
    }

    private static void closeAfterFailedCreation(List<StorageVolume> volumes, RuntimeException failure) {
        for (StorageVolume volume : volumes) {
            try {
                volume.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }
}
