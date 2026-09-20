package io.github.cocosip.stow.internal.journal;

import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.exception.JournalCorruptionException;
import java.nio.file.Path;
import java.time.Duration;

public final class JournalMemoryProbe {

    private JournalMemoryProbe() {}

    public static void main(String[] args) {
        JournalConfiguration configuration = new JournalConfiguration(
                true,
                true,
                JournalFormat.BINARY_V1,
                JournalAckMode.DURABLE,
                Duration.ZERO,
                Duration.ZERO,
                16,
                262_144,
                Duration.ofSeconds(30),
                8,
                Duration.ZERO);
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(Path.of(args[0]), configuration, new BinaryV1JournalCodec())) {
            journal.append(FileQueueEventJournalTest.event("tenant-a", 1));
            throw new AssertionError("corrupt journal was writable");
        } catch (JournalCorruptionException expected) {
            System.out.println("corruption isolated");
        }
    }
}
