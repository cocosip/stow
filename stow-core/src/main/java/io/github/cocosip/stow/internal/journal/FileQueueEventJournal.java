package io.github.cocosip.stow.internal.journal;

import io.github.cocosip.stow.config.JournalConfiguration;
import io.github.cocosip.stow.exception.DatabaseRecoveryException;
import io.github.cocosip.stow.exception.JournalCorruptionException;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.JournalCodec;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public final class FileQueueEventJournal implements QueueEventJournal {

    private final Path queueDirectory;
    private final JournalConfiguration configuration;
    private final JournalCodec codec;
    private final Map<String, TenantLog> tenants = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public FileQueueEventJournal(Path queueDirectory, JournalConfiguration configuration, JournalCodec codec) {
        this.queueDirectory = queueDirectory.toAbsolutePath().normalize();
        this.configuration = configuration;
        this.codec = codec;
        try {
            Files.createDirectories(this.queueDirectory);
            scanTenants();
        } catch (IOException exception) {
            throw new DatabaseRecoveryException("Unable to initialize journal", exception);
        }
    }

    @Override
    public long append(QueueEventRecord event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        TenantLog tenant = tenant(event.tenantId());
        TenantJournalWriter writer;
        CompletableFuture<Void> completion;
        long previousSequence;
        synchronized (tenant) {
            ensureOpen(tenant);
            if (event.sequenceNumber() != tenant.admittedSequence + 1) {
                throw new JournalCorruptionException(
                        "Journal sequence gap or regression for tenant " + event.tenantId());
            }
            previousSequence = tenant.admittedSequence;
            tenant.admittedSequence = event.sequenceNumber();
            ensureWriter(tenant);
            writer = tenant.writer;
            completion = writer.enqueue(List.of(event));
        }
        try {
            if (configuration.ackMode() != io.github.cocosip.stow.config.JournalAckMode.ASYNC) {
                awaitWrite(completion);
            }
        } catch (RuntimeException exception) {
            synchronized (tenant) {
                tenant.admittedSequence = previousSequence;
            }
            throw exception;
        }
        synchronized (tenant) {
            return tenant.tailOffset;
        }
    }

    @Override
    public long appendBatch(List<QueueEventRecord> events) {
        if (events == null || events.isEmpty()) {
            return 0;
        }
        String tenantId = events.get(0).tenantId();
        for (QueueEventRecord event : events) {
            if (!tenantId.equals(event.tenantId())) {
                throw new IllegalArgumentException("appendBatch must contain one tenant");
            }
        }
        TenantLog tenant = tenant(tenantId);
        TenantJournalWriter writer;
        CompletableFuture<Void> completion;
        long previousSequence;
        synchronized (tenant) {
            ensureOpen(tenant);
            long expected = tenant.admittedSequence + 1;
            previousSequence = tenant.admittedSequence;
            for (QueueEventRecord event : events) {
                if (event.sequenceNumber() != expected++) {
                    throw new JournalCorruptionException("Journal sequence gap or regression for tenant " + tenantId);
                }
            }
            tenant.admittedSequence = expected - 1;
            ensureWriter(tenant);
            writer = tenant.writer;
            completion = writer.enqueue(events);
        }
        try {
            if (configuration.ackMode() != io.github.cocosip.stow.config.JournalAckMode.ASYNC) {
                awaitWrite(completion);
            }
        } catch (RuntimeException exception) {
            synchronized (tenant) {
                tenant.admittedSequence = previousSequence;
            }
            throw exception;
        }
        synchronized (tenant) {
            return tenant.tailOffset;
        }
    }

    @Override
    public JournalReadBatch readBatch(String tenantId, long offset, int maxRecords) {
        if (offset < 0 || maxRecords <= 0) {
            throw new IllegalArgumentException("offset must be non-negative and maxRecords must be positive");
        }
        TenantLog tenant = tenant(tenantId);
        synchronized (tenant) {
            ensureOpen(tenant);
            List<QueueEventRecord> selected = new ArrayList<>();
            long next = offset;
            long last = 0;
            for (JournalScanner.Record record : tenant.records) {
                if (record.nextOffset() <= offset) {
                    continue;
                }
                if (selected.size() >= maxRecords) {
                    break;
                }
                selected.add(record.event());
                next = record.nextOffset();
                last = record.event().sequenceNumber();
            }
            if (selected.isEmpty()) {
                next = Math.max(offset, tenant.tailOffset);
                last = tenant.lastSequence;
            }
            return new JournalReadBatch(tenantId, offset, next, last, selected);
        }
    }

    @Override
    public long tailOffset(String tenantId) {
        TenantLog tenant = tenant(tenantId);
        synchronized (tenant) {
            return tenant.tailOffset;
        }
    }

    @Override
    public long baseOffset(String tenantId) {
        TenantLog tenant = tenant(tenantId);
        synchronized (tenant) {
            return tenant.baseOffset;
        }
    }

    @Override
    public Set<String> tenantIds() {
        return Collections.unmodifiableSet(new TreeSet<>(tenants.keySet()));
    }

    @Override
    public void compact(String tenantId, long throughOffset) {
        TenantLog tenant = tenant(tenantId);
        TenantJournalWriter writer;
        long previousBase;
        List<JournalScanner.Record> kept;
        Path log;
        synchronized (tenant) {
            ensureOpen(tenant);
            if (throughOffset < tenant.baseOffset || throughOffset > tenant.tailOffset) {
                throw new IllegalArgumentException("compaction offset is outside journal bounds");
            }
            if (throughOffset == tenant.baseOffset) {
                return;
            }
            writer = tenant.writer;
            tenant.writer = null;
            previousBase = tenant.baseOffset;
            kept = tenant.records.stream()
                    .filter(record -> record.nextOffset() > throughOffset)
                    .toList();
            log = tenant.directory.resolve("queue.log");
        }
        if (writer != null) {
            writer.flush();
            writer.close();
        }
        synchronized (tenant) {
            Path temp = tenant.directory.resolve(".queue.log.compact.tmp");
            try {
                byte[] source = Files.readAllBytes(log);
                Files.write(
                        temp,
                        java.util.Arrays.copyOfRange(source, (int) (throughOffset - previousBase), source.length),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE);
                Files.move(
                        temp,
                        log,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException exception) {
                throw new DatabaseRecoveryException("Unable to compact journal", exception);
            } finally {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException ignored) {
                    // Best effort cleanup.
                }
            }
            tenant.baseOffset = throughOffset;
            tenant.records = new ArrayList<>(kept);
            for (int i = 0; i < tenant.records.size(); i++) {
                JournalScanner.Record record = tenant.records.get(i);
                tenant.records.set(
                        i,
                        new JournalScanner.Record(
                                record.offset() - throughOffset, record.nextOffset() - throughOffset, record.event()));
            }
            tenant.tailOffset = tenant.baseOffset + FilesSize(log);
            persistState(tenant, false, -1);
        }
    }

    @Override
    public void flush() {
        for (TenantLog tenant : tenants.values()) {
            TenantJournalWriter writer;
            synchronized (tenant) {
                writer = tenant.writer;
            }
            if (writer != null) {
                writer.flush();
            }
            synchronized (tenant) {
                persistState(tenant, false, -1);
            }
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (TenantLog tenant : tenants.values()) {
            TenantJournalWriter writer;
            synchronized (tenant) {
                writer = tenant.writer;
                tenant.writer = null;
            }
            if (writer != null) {
                writer.close();
            }
            synchronized (tenant) {
                persistState(tenant, false, -1);
            }
        }
    }

    private void scanTenants() throws IOException {
        try (var directories = Files.list(queueDirectory)) {
            directories.filter(Files::isDirectory).forEach(directory -> {
                String tenantId = directory.getFileName().toString();
                try {
                    JournalCodec tenantCodec = codecFor(directory);
                    JournalScanner.Result result =
                            new JournalScanner().scan(directory.resolve("queue.log"), tenantCodec, true);
                    TenantLog tenant = tenantFromScan(tenantId, directory, result, tenantCodec);
                    tenants.put(tenantId, tenant);
                    persistState(tenant, result.repaired(), result.corruptOffset());
                } catch (JournalCorruptionException exception) {
                    TenantLog failed = new TenantLog(tenantId, directory, exception);
                    tenants.put(tenantId, failed);
                } catch (IOException exception) {
                    throw new DatabaseRecoveryException("Unable to scan tenant journal " + tenantId, exception);
                }
            });
        }
    }

    private TenantLog tenant(String tenantId) {
        validateTenantId(tenantId);
        return tenants.computeIfAbsent(tenantId, id -> {
            Path directory = queueDirectory.resolve(id);
            try {
                JournalCodec tenantCodec = codecFor(directory);
                JournalScanner.Result result =
                        new JournalScanner().scan(directory.resolve("queue.log"), tenantCodec, false);
                TenantLog tenant = tenantFromScan(id, directory, result, tenantCodec);
                persistState(tenant, result.repaired(), result.corruptOffset());
                return tenant;
            } catch (IOException exception) {
                throw new DatabaseRecoveryException("Unable to create tenant journal " + id, exception);
            }
        });
    }

    private static void validateTenantId(String tenantId) {
        if (tenantId == null
                || !tenantId.matches("[A-Za-z0-9._-]{1,128}")
                || tenantId.equals(".")
                || tenantId.equals("..")
                || java.util.Set.of(
                                "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7",
                                "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9")
                        .contains(tenantId.toUpperCase(java.util.Locale.ROOT))) {
            throw new IllegalArgumentException("tenantId is not a valid identifier");
        }
    }

    private JournalCodec codecFor(Path directory) throws IOException {
        Path log = directory.resolve("queue.log");
        if (Files.exists(log) && Files.size(log) > 0) {
            return new JournalFormatDetector().detect(log) == io.github.cocosip.stow.config.JournalFormat.BINARY_V1
                    ? new BinaryV1JournalCodec()
                    : new JsonLinesJournalCodec();
        }
        return codec;
    }

    private TenantLog tenantFromScan(
            String tenantId, Path directory, JournalScanner.Result result, JournalCodec tenantCodec) {
        TenantLog tenant = new TenantLog(tenantId, directory, result, tenantCodec);
        try {
            JournalStateStore.State state = new JournalStateStore(directory).load();
            if (state != null
                    && state.format() == tenantCodec.format()
                    && state.tailOffset() == state.baseOffset() + result.physicalLength()
                    && state.lastSequenceNumber() == result.lastSequenceNumber()) {
                tenant.baseOffset = state.baseOffset();
                tenant.tailOffset = state.tailOffset();
                tenant.repairCount = state.repairCount();
                tenant.records = result.records().stream()
                        .map(record -> new JournalScanner.Record(
                                record.offset() + tenant.baseOffset,
                                record.nextOffset() + tenant.baseOffset,
                                record.event()))
                        .collect(java.util.stream.Collectors.toCollection(ArrayList::new));
            }
        } catch (IOException | RuntimeException ignored) {
            // State is an acceleration file; the physical log remains authoritative.
        }
        return tenant;
    }

    private void ensureWriter(TenantLog tenant) {
        if (tenant.writer == null) {
            tenant.writer = new TenantJournalWriter(
                    tenant.tenantId,
                    tenant.directory,
                    tenant.codec,
                    configuration,
                    tenant.tailOffset - tenant.baseOffset,
                    result -> {
                        synchronized (tenant) {
                            tenant.records.addAll(result.records().stream()
                                    .map(record -> new JournalScanner.Record(
                                            record.offset() + tenant.baseOffset,
                                            record.nextOffset() + tenant.baseOffset,
                                            record.event()))
                                    .toList());
                            tenant.tailOffset = tenant.baseOffset + result.tailOffset();
                            tenant.lastSequence = result.records()
                                    .get(result.records().size() - 1)
                                    .event()
                                    .sequenceNumber();
                            persistState(tenant, false, -1);
                        }
                    });
        }
    }

    private static void awaitWrite(CompletableFuture<Void> completion) {
        try {
            completion.join();
        } catch (java.util.concurrent.CompletionException exception) {
            if (exception.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new DatabaseRecoveryException("Journal write failed", exception.getCause());
        }
    }

    private void ensureOpen(TenantLog tenant) {
        if (closed) {
            throw new IllegalStateException("Journal is closed");
        }
        if (tenant.failure != null) {
            throw tenant.failure;
        }
    }

    private void persistState(TenantLog tenant, boolean repaired, long corruptOffset) {
        try {
            new JournalStateStore(tenant.directory)
                    .write(new JournalStateStore.State(
                            1,
                            tenant.codec.format(),
                            tenant.baseOffset,
                            tenant.tailOffset,
                            tenant.lastSequence,
                            repaired,
                            corruptOffset >= 0 ? corruptOffset : null,
                            repaired ? tenant.repairCount + 1 : tenant.repairCount,
                            Instant.now()));
            if (repaired) {
                tenant.repairCount++;
            }
        } catch (IOException exception) {
            throw new DatabaseRecoveryException("Unable to persist journal state", exception);
        }
    }

    private static long FilesSize(Path path) {
        try {
            return Files.size(path);
        } catch (IOException exception) {
            throw new DatabaseRecoveryException("Unable to inspect compacted journal", exception);
        }
    }

    private static final class TenantLog {
        private final String tenantId;
        private final Path directory;
        private final JournalCodec codec;
        private List<JournalScanner.Record> records;
        private long baseOffset;
        private long tailOffset;
        private long lastSequence;
        private long admittedSequence;
        private long repairCount;
        private JournalCorruptionException failure;
        private TenantJournalWriter writer;

        private TenantLog(String tenantId, Path directory, JournalScanner.Result result, JournalCodec codec) {
            this.tenantId = tenantId;
            this.directory = directory;
            this.codec = codec;
            this.records = new ArrayList<>(result.records());
            this.tailOffset = result.physicalLength();
            this.lastSequence = result.lastSequenceNumber();
            this.admittedSequence = this.lastSequence;
        }

        private TenantLog(String tenantId, Path directory, JournalCorruptionException failure) {
            this.tenantId = tenantId;
            this.directory = directory;
            this.codec = new BinaryV1JournalCodec();
            this.records = new ArrayList<>();
            this.failure = failure;
        }
    }
}
