package io.github.cocosip.stow.internal.cleanup;

import io.github.cocosip.stow.internal.journal.SequencedJournalAppender;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.util.Objects;

/** Serializes maintenance events per tenant and assigns the next journal sequence. */
public final class MaintenanceEventAppender {

    private final SequencedJournalAppender appender;

    public MaintenanceEventAppender(QueueEventJournal journal) {
        this(new SequencedJournalAppender(journal));
    }

    public MaintenanceEventAppender(SequencedJournalAppender appender) {
        this.appender = Objects.requireNonNull(appender, "appender");
    }

    public QueueEventRecord append(QueueEventRecord event) {
        return appender.append(event);
    }
}
