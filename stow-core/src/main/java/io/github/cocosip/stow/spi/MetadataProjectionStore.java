package io.github.cocosip.stow.spi;

@ExperimentalApi
public interface MetadataProjectionStore extends AutoCloseable {

    @Override
    void close();
}
