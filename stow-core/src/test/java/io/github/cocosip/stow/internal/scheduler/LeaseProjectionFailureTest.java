package io.github.cocosip.stow.internal.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.cocosip.stow.api.ContentSources;
import io.github.cocosip.stow.config.JournalAckMode;
import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.internal.filesystem.DefaultStorageVolumeProvider;
import io.github.cocosip.stow.internal.journal.BinaryV1JournalCodec;
import io.github.cocosip.stow.internal.journal.FileQueueEventJournal;
import io.github.cocosip.stow.internal.journal.JournalReadBatch;
import io.github.cocosip.stow.internal.projection.ProjectionCursorStore;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.QueueProjectionService;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.ProcessingLease;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class LeaseProjectionFailureTest {

    @Test
    void durableCompletionRemainsSuccessfulAndIdempotentWhenProjectionFails() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "lease-projection-failure-");
        Clock clock = Clock.fixed(Instant.parse("2026-09-20T00:00:00Z"), ZoneOffset.UTC);
        TenantContext tenant = new TenantContext("tenant-a", TenantStatus.ENABLED, clock.instant(), clock.instant());
        SqliteQuotaRepository quota = new SqliteQuotaRepository(
                root.resolve("quota"), SqliteConnectionFactory.defaults(), clock, ignored -> 10);
        SqliteMetadataProjectionStore metadata =
                new SqliteMetadataProjectionStore(root.resolve("metadata"), SqliteConnectionFactory.defaults(), clock);
        AtomicReference<RuntimeException> observedFailure = new AtomicReference<>();

        try (FileQueueEventJournal journal = new FileQueueEventJournal(
                        root.resolve("journal"), configuration(), new BinaryV1JournalCodec());
                QueueEventJournal injectedJournal = new FailNextReadJournal(journal);
                var volume = new DefaultStorageVolumeProvider()
                        .create(new VolumeConfiguration("volume-a", root.resolve("volume"), 0, 8_192, true))) {
            FailNextReadJournal failingJournal = (FailNextReadJournal) injectedJournal;
            QueueProjectionService projection = new QueueProjectionService(
                    failingJournal,
                    new QueueEventReducer(metadata, quota),
                    new ProjectionCursorStore(root.resolve("cursor"), clock),
                    clock,
                    null,
                    observedFailure::set);
            DefaultStoragePool pool = new DefaultStoragePool(
                    ignored -> tenant, quota, metadata, projection, failingJournal, volume, clock);

            pool.write(tenant, ContentSources.of(new byte[] {1}), null);
            ProcessingLease lease = pool.claimNext(tenant).orElseThrow().lease();
            failingJournal.failNextRead();

            assertThatCode(() -> pool.complete(lease)).doesNotThrowAnyException();
            assertThatCode(() -> pool.complete(lease)).doesNotThrowAnyException();

            List<QueueEventRecord> events = journal.readBatch("tenant-a", journal.baseOffset("tenant-a"), 20)
                    .events();
            assertThat(events)
                    .filteredOn(event -> event.eventType() == QueueEventType.PROCESSING_COMPLETED)
                    .hasSize(1);
            assertThat(observedFailure.get()).hasMessage("injected projection read failure");

            projection.projectTenantUntilCaughtUp("tenant-a", 20);
            assertThat(metadata.find("tenant-a", lease.fileKey()).orElseThrow().status())
                    .isEqualTo(FileProcessingStatus.COMPLETED);
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
                16,
                262_144,
                Duration.ofSeconds(30),
                16,
                Duration.ZERO);
    }

    private static final class FailNextReadJournal implements QueueEventJournal {
        private final QueueEventJournal delegate;
        private final AtomicBoolean failNextRead = new AtomicBoolean();

        private FailNextReadJournal(QueueEventJournal delegate) {
            this.delegate = delegate;
        }

        private void failNextRead() {
            failNextRead.set(true);
        }

        @Override
        public long append(QueueEventRecord event) {
            return delegate.append(event);
        }

        @Override
        public long appendBatch(List<QueueEventRecord> events) {
            return delegate.appendBatch(events);
        }

        @Override
        public JournalReadBatch readBatch(String tenantId, long offset, int maxRecords) {
            JournalReadBatch batch = delegate.readBatch(tenantId, offset, maxRecords);
            if (failNextRead.get()
                    && batch.events().stream()
                            .anyMatch(event -> event.eventType() == QueueEventType.PROCESSING_COMPLETED)
                    && failNextRead.compareAndSet(true, false)) {
                throw new IllegalStateException("injected projection read failure");
            }
            return batch;
        }

        @Override
        public long tailOffset(String tenantId) {
            return delegate.tailOffset(tenantId);
        }

        @Override
        public long baseOffset(String tenantId) {
            return delegate.baseOffset(tenantId);
        }

        @Override
        public Set<String> tenantIds() {
            return delegate.tenantIds();
        }

        @Override
        public void compact(String tenantId, long throughOffset) {
            delegate.compact(tenantId, throughOffset);
        }

        @Override
        public void flush() {
            delegate.flush();
        }

        @Override
        public void close() {}
    }
}
