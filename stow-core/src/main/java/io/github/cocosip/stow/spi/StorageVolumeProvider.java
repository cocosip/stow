package io.github.cocosip.stow.spi;

import io.github.cocosip.stow.config.VolumeConfiguration;

public interface StorageVolumeProvider {

    StorageVolume create(VolumeConfiguration configuration);
}
