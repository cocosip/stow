package io.github.cocosip.stow.internal.filesystem;

import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.spi.StorageVolume;
import io.github.cocosip.stow.spi.StorageVolumeProvider;
import java.util.Objects;

public final class DefaultStorageVolumeProvider implements StorageVolumeProvider {

    @Override
    public StorageVolume create(VolumeConfiguration configuration) {
        return new LocalFileSystemVolume(Objects.requireNonNull(configuration, "configuration"));
    }
}
