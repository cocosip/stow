package io.github.cocosip.stow.internal.journal;

import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.JournalCodec;

public final class BinaryV1JournalCodec implements JournalCodec {

    @Override
    public JournalFormat format() {
        return JournalFormat.BINARY_V1;
    }

    @Override
    public byte[] encode(QueueEventRecord event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        return JournalFrame.encode(event.sequenceNumber(), QueueEventJson.encode(event));
    }

    @Override
    public QueueEventRecord decode(byte[] payload) {
        JournalFrame frame = JournalFrame.decode(payload);
        QueueEventRecord event = QueueEventJson.decode(frame.payload());
        if (frame.sequenceNumber() != event.sequenceNumber()) {
            throw new io.github.cocosip.stow.exception.JournalCorruptionException(
                    "Journal frame sequence does not match queue event sequence");
        }
        return event;
    }
}
