package io.github.cocosip.stow.internal.journal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.config.JournalFormat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class JournalCrashRecoveryTest {

    @Test
    void truncatesAnIncompleteFinalBinaryFrame() throws Exception {
        Path root = Files.createTempDirectory("stow-journal-recovery");
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
                new FileQueueEventJournal(root, configuration, new BinaryV1JournalCodec())) {
            journal.append(FileQueueEventJournalTest.event("tenant-a", 1));
        }
        Path log = root.resolve("tenant-a").resolve("queue.log");
        Files.write(log, new byte[] {1, 2, 3}, java.nio.file.StandardOpenOption.APPEND);
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration, new BinaryV1JournalCodec())) {
            assertThat(journal.readBatch("tenant-a", 0, 10).events()).hasSize(1);
        }
        assertThat(Files.size(log)).isGreaterThan(3);
    }

    @Test
    void truncatesBadFinalCrcButIsolatesMiddleCorruption() throws Exception {
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

        Path tailRoot = Files.createTempDirectory("stow-journal-crc-tail");
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(tailRoot, configuration, new BinaryV1JournalCodec())) {
            journal.append(FileQueueEventJournalTest.event("tenant-a", 1));
        }
        Path tailLog = tailRoot.resolve("tenant-a").resolve("queue.log");
        byte[] tailBytes = Files.readAllBytes(tailLog);
        tailBytes[tailBytes.length - 1] ^= 0x01;
        Files.write(tailLog, tailBytes);
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(tailRoot, configuration, new BinaryV1JournalCodec())) {
            assertThat(journal.readBatch("tenant-a", 0, 10).events()).isEmpty();
        }

        Path middleRoot = Files.createTempDirectory("stow-journal-crc-middle");
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(middleRoot, configuration, new BinaryV1JournalCodec())) {
            journal.append(FileQueueEventJournalTest.event("tenant-a", 1));
            journal.append(FileQueueEventJournalTest.event("tenant-a", 2));
        }
        Path middleLog = middleRoot.resolve("tenant-a").resolve("queue.log");
        byte[] middleBytes = Files.readAllBytes(middleLog);
        middleBytes[24] ^= 0x01;
        Files.write(middleLog, middleBytes);
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(middleRoot, configuration, new BinaryV1JournalCodec())) {
            assertThatThrownBy(() -> journal.append(FileQueueEventJournalTest.event("tenant-a", 3)))
                    .isInstanceOf(RuntimeException.class);
        }
    }
}
