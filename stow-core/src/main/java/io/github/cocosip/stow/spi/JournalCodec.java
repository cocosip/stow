package io.github.cocosip.stow.spi;

import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.model.QueueEventRecord;

public interface JournalCodec {

    JournalFormat format();

    byte[] encode(QueueEventRecord event);

    QueueEventRecord decode(byte[] payload);
}
