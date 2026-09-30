package io.github.cocosip.stow.internal.journal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Observed journal write-path counters (Locus QueueJournalWritePathStatistics /
 * QueueJournalMetrics): append batches and sizes, forces, and corrupt-tail handling.
 */
public record QueueJournalWritePathStatistics(
        long appendBatchCount,
        long singleRecordAppendBatches,
        long multiRecordAppendBatches,
        long appendedRecordCount,
        long appendedBytes,
        long appendNanos,
        long flushCount,
        long flushNanos,
        long corruptTailsDetected,
        long corruptTailsRepaired) {

    public static QueueJournalWritePathStatistics empty() {
        return new QueueJournalWritePathStatistics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    static final class Builder {

        private final AtomicLong appendBatchCount = new AtomicLong();
        private final AtomicLong singleRecordAppendBatches = new AtomicLong();
        private final AtomicLong multiRecordAppendBatches = new AtomicLong();
        private final AtomicLong appendedRecordCount = new AtomicLong();
        private final AtomicLong appendedBytes = new AtomicLong();
        private final AtomicLong appendNanos = new AtomicLong();
        private final AtomicLong flushCount = new AtomicLong();
        private final AtomicLong flushNanos = new AtomicLong();
        private final AtomicLong corruptTailsDetected = new AtomicLong();
        private final AtomicLong corruptTailsRepaired = new AtomicLong();

        QueueJournalWritePathStatistics.Builder appendBatchCount(long value) {
            appendBatchCount.addAndGet(value);
            return this;
        }

        QueueJournalWritePathStatistics.Builder singleRecordAppendBatches(long value) {
            singleRecordAppendBatches.addAndGet(value);
            return this;
        }

        QueueJournalWritePathStatistics.Builder multiRecordAppendBatches(long value) {
            multiRecordAppendBatches.addAndGet(value);
            return this;
        }

        QueueJournalWritePathStatistics.Builder appendedRecordCount(long value) {
            appendedRecordCount.addAndGet(value);
            return this;
        }

        QueueJournalWritePathStatistics.Builder appendedBytes(long value) {
            appendedBytes.addAndGet(value);
            return this;
        }

        QueueJournalWritePathStatistics.Builder appendNanos(long value) {
            appendNanos.addAndGet(value);
            return this;
        }

        QueueJournalWritePathStatistics.Builder flushCount(long value) {
            flushCount.addAndGet(value);
            return this;
        }

        QueueJournalWritePathStatistics.Builder flushNanos(long value) {
            flushNanos.addAndGet(value);
            return this;
        }

        void corruptTailDetected() {
            corruptTailsDetected.incrementAndGet();
        }

        void corruptTailRepaired() {
            corruptTailsRepaired.incrementAndGet();
        }

        QueueJournalWritePathStatistics build() {
            return new QueueJournalWritePathStatistics(
                    appendBatchCount.get(),
                    singleRecordAppendBatches.get(),
                    multiRecordAppendBatches.get(),
                    appendedRecordCount.get(),
                    appendedBytes.get(),
                    appendNanos.get(),
                    flushCount.get(),
                    flushNanos.get(),
                    corruptTailsDetected.get(),
                    corruptTailsRepaired.get());
        }
    }
}
