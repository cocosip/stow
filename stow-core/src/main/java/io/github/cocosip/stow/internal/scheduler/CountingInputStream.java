package io.github.cocosip.stow.internal.scheduler;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

final class CountingInputStream extends FilterInputStream {

    private long count;

    CountingInputStream(InputStream input) {
        super(input);
    }

    long count() {
        return count;
    }

    @Override
    public int read() throws IOException {
        int value = super.read();
        if (value >= 0) count++;
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        int read = super.read(bytes, offset, length);
        if (read > 0) count += read;
        return read;
    }
}
