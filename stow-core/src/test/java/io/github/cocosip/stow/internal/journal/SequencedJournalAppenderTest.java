package io.github.cocosip.stow.internal.journal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.internal.cleanup.MaintenanceEventAppender;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class SequencedJournalAppenderTest {

    @Test
    void assignsOneGapFreeSequenceAcrossConcurrentProducers() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "sequenced-appender-");
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration(), new BinaryV1JournalCodec())) {
            SequencedJournalAppender appender = new SequencedJournalAppender(journal);
            MaintenanceEventAppender maintenance = new MaintenanceEventAppender(appender);
            try (var executor = Executors.newFixedThreadPool(8)) {
                List<java.util.concurrent.Future<?>> writes = new ArrayList<>();
                for (int index = 0; index < 200; index++) {
                    int fileNumber = index;
                    writes.add(executor.submit(() -> {
                        if (fileNumber % 2 == 0) appender.append(event(fileNumber));
                        else maintenance.append(event(fileNumber));
                    }));
                }
                for (var write : writes) write.get();
            }

            journal.flush();
            assertThat(journal.readBatch("tenant-a", journal.baseOffset("tenant-a"), 300)
                            .events())
                    .extracting(QueueEventRecord::sequenceNumber)
                    .containsExactlyElementsOf(java.util.stream.LongStream.rangeClosed(1, 200)
                            .boxed()
                            .toList());
        }
    }

    private static JournalConfiguration configuration() {
        return new JournalConfiguration(
                true,
                true,
                JournalFormat.BINARY_V1,
                JournalAckMode.DURABLE,
                Duration.ZERO,
                Duration.ZERO,
                32,
                262_144,
                Duration.ofSeconds(30),
                256,
                Duration.ZERO);
    }

    private static QueueEventRecord event(int fileNumber) {
        return new QueueEventRecord(
                1,
                UUID.randomUUID(),
                "tenant-a",
                "%032x".formatted(fileNumber),
                QueueEventType.ACCEPTED,
                Instant.parse("2026-09-20T00:00:00Z"),
                1,
                "volume-a",
                Path.of("volume", "%032x".formatted(fileNumber)),
                "/",
                1,
                FileProcessingStatus.PENDING,
                null,
                null,
                0,
                null,
                null,
                null,
                "");
    }
}
