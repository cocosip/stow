package io.github.cocosip.stow.internal.journal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class FileQueueEventJournalTest {

    @Test
    void appendsReadsAndReopensTenantLog() throws Exception {
        Path root = Files.createTempDirectory("stow-journal");
        JournalConfiguration configuration = configuration(JournalAckMode.DURABLE);
        QueueEventRecord event = event("tenant-a", 1);

        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration, new BinaryV1JournalCodec())) {
            assertThat(journal.append(event)).isGreaterThan(0);
            assertThat(journal.readBatch("tenant-a", 0, 10).events()).containsExactly(event);
            assertThat(journal.tailOffset("tenant-a")).isPositive();
        }

        try (FileQueueEventJournal reopened =
                new FileQueueEventJournal(root, configuration, new BinaryV1JournalCodec())) {
            assertThat(reopened.readBatch("tenant-a", 0, 10).events()).containsExactly(event);
        }
    }

    @Test
    void returnsEmptyBatchWhenReadStartsBeyondTail() throws Exception {
        Path root = Files.createTempDirectory("stow-journal-read-beyond-tail");
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration(JournalAckMode.DURABLE), new BinaryV1JournalCodec())) {
            journal.append(event("tenant-a", 1));
            long offset = journal.tailOffset("tenant-a") + 17;

            JournalReadBatch batch = journal.readBatch("tenant-a", offset, 10);

            assertThat(batch.events()).isEmpty();
            assertThat(batch.startOffset()).isEqualTo(offset);
            assertThat(batch.nextOffset()).isEqualTo(offset);
            assertThat(batch.lastSequenceNumber()).isEqualTo(1);
        }
    }

    @Test
    void preservesTenantOrderAndAllowsIndependentTenants() throws Exception {
        Path root = Files.createTempDirectory("stow-journal-order");
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration(JournalAckMode.ASYNC, 64), new BinaryV1JournalCodec())) {
            IntStream.rangeClosed(1, 5).forEach(sequence -> journal.append(event("tenant-a", sequence)));
            IntStream.rangeClosed(1, 5).forEach(sequence -> journal.append(event("tenant-b", sequence)));
            journal.flush();
            assertThat(journal.tenantIds()).containsExactly("tenant-a", "tenant-b");
            assertThat(journal.readBatch("tenant-a", 0, 10).events())
                    .extracting(QueueEventRecord::sequenceNumber)
                    .containsExactly(1L, 2L, 3L, 4L, 5L);
            assertThat(journal.readBatch("tenant-b", 0, 10).events())
                    .extracting(QueueEventRecord::sequenceNumber)
                    .containsExactly(1L, 2L, 3L, 4L, 5L);
        }
    }

    @Test
    void supportsEveryAcknowledgementMode() throws Exception {
        for (JournalAckMode mode : JournalAckMode.values()) {
            Path root = Files.createTempDirectory("stow-journal-ack");
            try (FileQueueEventJournal journal =
                    new FileQueueEventJournal(root, configuration(mode), new BinaryV1JournalCodec())) {
                journal.append(event("tenant-a", 1));
                journal.flush();
                assertThat(journal.readBatch("tenant-a", 0, 1).events()).hasSize(1);
            }
        }
    }

    @Test
    void rejectsSequenceGaps() throws Exception {
        Path root = Files.createTempDirectory("stow-journal-gap");
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration(JournalAckMode.DURABLE), new BinaryV1JournalCodec())) {
            assertThatThrownBy(() -> journal.append(event("tenant-a", 2))).isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void rebuildsMissingState() throws Exception {
        Path root = Files.createTempDirectory("stow-journal-state");
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration(JournalAckMode.DURABLE), new BinaryV1JournalCodec())) {
            journal.append(event("tenant-a", 1));
        }
        Files.delete(root.resolve("tenant-a").resolve("queue.state.json"));
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration(JournalAckMode.DURABLE), new BinaryV1JournalCodec())) {
            assertThat(journal.readBatch("tenant-a", 0, 1).events()).hasSize(1);
            assertThat(Files.exists(root.resolve("tenant-a").resolve("queue.state.json")))
                    .isTrue();
        }
    }

    @Test
    void rejectsBatchLargerThanRecordCapacity() throws Exception {
        Path root = Files.createTempDirectory("stow-journal-capacity");
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration(JournalAckMode.ASYNC, 2), new BinaryV1JournalCodec())) {
            assertThatThrownBy(() -> journal.appendBatch(
                            List.of(event("tenant-a", 1), event("tenant-a", 2), event("tenant-a", 3))))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void reopensCompactedSuffixUsingPersistedBaseOffset() throws Exception {
        Path root = Files.createTempDirectory("stow-journal-compact");
        JournalConfiguration configuration = configuration(JournalAckMode.DURABLE, 8);
        long firstEnd;
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration, new BinaryV1JournalCodec())) {
            journal.append(event("tenant-a", 1));
            journal.append(event("tenant-a", 2));
            journal.append(event("tenant-a", 3));
            firstEnd = journal.readBatch("tenant-a", 0, 1).nextOffset();
            journal.compact("tenant-a", firstEnd);
            assertThat(journal.baseOffset("tenant-a")).isEqualTo(firstEnd);
            assertThat(journal.readBatch("tenant-a", firstEnd, 10).events())
                    .extracting(QueueEventRecord::sequenceNumber)
                    .containsExactly(2L, 3L);
        }
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration, new BinaryV1JournalCodec())) {
            assertThat(journal.readBatch("tenant-a", firstEnd, 10).events())
                    .extracting(QueueEventRecord::sequenceNumber)
                    .containsExactly(2L, 3L);
        }
    }

    @Test
    void removesAbandonedCompactionTempWithoutChangingTheJournal() throws Exception {
        Path root = Files.createTempDirectory("stow-journal-compact-temp");
        JournalConfiguration configuration = configuration(JournalAckMode.DURABLE, 8);
        Path tenantDirectory = root.resolve("tenant-a");
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration, new BinaryV1JournalCodec())) {
            journal.append(event("tenant-a", 1));
            journal.append(event("tenant-a", 2));
        }

        Path abandoned = tenantDirectory.resolve(".queue.log.compact.tmp");
        Files.write(abandoned, new byte[] {1, 2, 3});
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration, new BinaryV1JournalCodec())) {
            assertThat(journal.readBatch("tenant-a", 0, 10).events())
                    .extracting(QueueEventRecord::sequenceNumber)
                    .containsExactly(1L, 2L);
            assertThat(abandoned).doesNotExist();
        }
    }

    @Test
    void recoversLogicalBaseWhenCrashOccursAfterLogReplaceBeforeStateReplace() throws Exception {
        Path root = Files.createTempDirectory("stow-journal-compact-recovery");
        JournalConfiguration configuration = configuration(JournalAckMode.DURABLE, 8);
        long firstEnd;
        long tail;
        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration, new BinaryV1JournalCodec())) {
            journal.append(event("tenant-a", 1));
            journal.append(event("tenant-a", 2));
            journal.append(event("tenant-a", 3));
            firstEnd = journal.readBatch("tenant-a", 0, 1).nextOffset();
            tail = journal.tailOffset("tenant-a");
        }

        Path log = root.resolve("tenant-a").resolve("queue.log");
        byte[] original = Files.readAllBytes(log);
        Files.write(log, java.util.Arrays.copyOfRange(original, Math.toIntExact(firstEnd), original.length));

        try (FileQueueEventJournal journal =
                new FileQueueEventJournal(root, configuration, new BinaryV1JournalCodec())) {
            assertThat(journal.baseOffset("tenant-a")).isEqualTo(firstEnd);
            assertThat(journal.tailOffset("tenant-a")).isEqualTo(tail);
            assertThat(journal.readBatch("tenant-a", firstEnd, 10).events())
                    .extracting(QueueEventRecord::sequenceNumber)
                    .containsExactly(2L, 3L);
        }
    }

    @Test
    void reportsOversizedSparseCorruptionWithinAConstrainedHeap() throws Exception {
        Path root = Files.createTempDirectory("stow-journal-sparse");
        Path tenant = root.resolve("tenant-a");
        Files.createDirectories(tenant);
        try (FileChannel channel =
                FileChannel.open(tenant.resolve("queue.log"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.position(128L * 1024 * 1024 - 1);
            channel.write(ByteBuffer.wrap(new byte[] {0}));
        }
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Path javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java.exe");
        Process process = new ProcessBuilder(
                        javaExecutable.toString(),
                        "-Xmx32m",
                        "-cp",
                        classpath,
                        JournalMemoryProbe.class.getName(),
                        root.toString())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);

        assertThat(process.waitFor()).as(output).isZero();
        assertThat(output).doesNotContain("OutOfMemoryError");
    }

    private static JournalConfiguration configuration(JournalAckMode mode) {
        return configuration(mode, 8);
    }

    private static JournalConfiguration configuration(JournalAckMode mode, int capacity) {
        return new JournalConfiguration(
                true,
                true,
                JournalFormat.BINARY_V1,
                mode,
                Duration.ZERO,
                Duration.ZERO,
                16,
                262_144,
                Duration.ofSeconds(30),
                capacity,
                Duration.ZERO);
    }

    static QueueEventRecord event(String tenant, long sequence) {
        return new QueueEventRecord(
                1,
                UUID.randomUUID(),
                tenant,
                "0123456789abcdef0123456789abcdef",
                QueueEventType.ACCEPTED,
                Instant.parse("2026-01-01T00:00:00Z"),
                sequence,
                "volume-a",
                Path.of("/tmp/file"),
                "/",
                12,
                FileProcessingStatus.PENDING,
                null,
                null,
                0,
                null,
                null,
                null,
                null);
    }
}
