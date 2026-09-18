package io.github.cocosip.stow.internal.filesystem;

import io.github.cocosip.stow.exception.InsufficientStorageException;
import io.github.cocosip.stow.spi.StorageVolume;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.random.RandomGenerator;

final class PowerOfTwoVolumeSelector {

    private final VolumeRegistry registry;
    private final RandomGenerator random;

    PowerOfTwoVolumeSelector(VolumeRegistry registry) {
        this(registry, RandomGenerator.getDefault());
    }

    PowerOfTwoVolumeSelector(VolumeRegistry registry, RandomGenerator random) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.random = Objects.requireNonNull(random, "random");
    }

    synchronized List<StorageVolume> orderedWriteCandidates(long requiredBytes) {
        List<StorageVolume> writable = registry.writableVolumes(requiredBytes);
        if (writable.isEmpty()) {
            throw new InsufficientStorageException("No healthy storage volume has enough available capacity");
        }
        if (writable.size() == 1) {
            return writable;
        }

        int firstIndex = random.nextInt(writable.size());
        int secondIndex = random.nextInt(writable.size() - 1);
        if (secondIndex >= firstIndex) {
            secondIndex++;
        }
        StorageVolume first = writable.get(firstIndex);
        StorageVolume second = writable.get(secondIndex);
        StorageVolume primary = first.availableCapacity() >= second.availableCapacity() ? first : second;
        StorageVolume secondary = primary == first ? second : first;

        List<StorageVolume> ordered = new ArrayList<>(writable.size());
        ordered.add(primary);
        ordered.add(secondary);
        writable.stream()
                .filter(volume -> volume != primary && volume != secondary)
                .sorted(Comparator.comparingLong(StorageVolume::availableCapacity)
                        .reversed()
                        .thenComparing(StorageVolume::id))
                .forEach(ordered::add);
        return List.copyOf(ordered);
    }
}
