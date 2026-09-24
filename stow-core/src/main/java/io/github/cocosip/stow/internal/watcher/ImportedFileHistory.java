package io.github.cocosip.stow.internal.watcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Append-only import history with atomic pruning and post-action retry state. */
public final class ImportedFileHistory {

    private static final Logger LOG = LoggerFactory.getLogger(ImportedFileHistory.class);
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private final Path directory;
    private final Clock clock;
    private final ObjectMapper mapper;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, Instant> lastPrunedAt = new ConcurrentHashMap<>();

    public ImportedFileHistory(Path historyDirectory, Clock clock) {
        directory = Objects.requireNonNull(historyDirectory, "historyDirectory")
                .toAbsolutePath()
                .normalize();
        this.clock = Objects.requireNonNull(clock, "clock");
        mapper = new ObjectMapper()
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        try {
            Files.createDirectories(directory);
            cleanupTemporaryFiles();
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to initialize watcher history", exception);
        }
    }

    public Optional<HistoryEntry> find(String watcherId, Path source, long size, long modifiedAtMillis) {
        lock.lock();
        try {
            return readEntries(watcherId).stream()
                    .filter(entry -> entry.sourcePath().equals(normalize(source).toString()))
                    .filter(entry -> entry.size() == size && entry.modifiedAtMillis() == modifiedAtMillis)
                    .max(Comparator.comparing(HistoryEntry::recordedAt));
        } finally {
            lock.unlock();
        }
    }

    public Optional<HistoryEntry> find(String watcherId, SourceFingerprint fingerprint) {
        Objects.requireNonNull(fingerprint, "fingerprint");
        lock.lock();
        try {
            return readEntries(watcherId).stream()
                    .filter(entry -> fingerprint.equals(entry.fingerprint()))
                    .max(Comparator.comparing(HistoryEntry::recordedAt));
        } finally {
            lock.unlock();
        }
    }

    public void recordImported(
            String watcherId, Path source, long size, long modifiedAtMillis, String fileKey, boolean actionCompleted) {
        append(
                watcherId,
                new HistoryEntry(
                        normalize(source).toString(),
                        size,
                        modifiedAtMillis,
                        fileKey,
                        actionCompleted,
                        clock.instant(),
                        null));
    }

    public void recordImported(
            String watcherId, SourceFingerprint fingerprint, String fileKey, boolean actionCompleted) {
        append(
                watcherId,
                new HistoryEntry(
                        fingerprint.path(),
                        fingerprint.size(),
                        fingerprint.lastModifiedMillis(),
                        fileKey,
                        actionCompleted,
                        clock.instant(),
                        fingerprint));
    }

    public void recordActionCompleted(String watcherId, Path source, long size, long modifiedAtMillis, String fileKey) {
        recordImported(watcherId, source, size, modifiedAtMillis, fileKey, true);
    }

    public void recordActionCompleted(String watcherId, SourceFingerprint fingerprint, String fileKey) {
        recordImported(watcherId, fingerprint, fileKey, true);
    }

    public void prune(String watcherId, Duration retention) {
        prune(watcherId, retention, Duration.ZERO);
    }

    public void prune(String watcherId, Duration retention, Duration throttle) {
        Objects.requireNonNull(retention, "retention");
        Objects.requireNonNull(throttle, "throttle");
        if (retention.isNegative()) throw new IllegalArgumentException("retention must not be negative");
        if (throttle.isNegative()) throw new IllegalArgumentException("throttle must not be negative");
        Instant now = clock.instant();
        Instant previous = lastPrunedAt.get(watcherId);
        if (!throttle.isZero() && previous != null && now.isBefore(previous.plus(throttle))) return;
        lock.lock();
        try {
            List<HistoryEntry> entries = readEntries(watcherId);
            Instant cutoff = now.minus(retention);
            List<HistoryEntry> retained = entries.stream()
                    .filter(entry -> !entry.recordedAt().isBefore(cutoff))
                    .toList();
            lastPrunedAt.put(watcherId, now);
            if (retained.size() == entries.size()) return;
            writeEntries(watcherId, retained);
        } finally {
            lock.unlock();
        }
    }

    public List<HistoryEntry> read(String watcherId) {
        lock.lock();
        try {
            return List.copyOf(readEntries(watcherId));
        } finally {
            lock.unlock();
        }
    }

    private void append(String watcherId, HistoryEntry entry) {
        validateWatcherId(watcherId);
        lock.lock();
        try {
            Files.createDirectories(directory);
            Path file = historyPath(watcherId);
            byte[] bytes = (mapper.writeValueAsString(entry) + "\n").getBytes(StandardCharsets.UTF_8);
            try (FileChannel channel = FileChannel.open(
                    file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to append watcher history", exception);
        } finally {
            lock.unlock();
        }
    }

    private List<HistoryEntry> readEntries(String watcherId) {
        validateWatcherId(watcherId);
        Path file = historyPath(watcherId);
        if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)) return List.of();
        List<String> lines;
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            // malformed bytes decode to U+FFFD instead of throwing; a torn final line then fails JSON parsing below
            lines = reader.lines().toList();
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to read watcher history", exception);
        }
        List<HistoryEntry> entries = new ArrayList<>(lines.size());
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) continue;
            try {
                entries.add(mapper.readValue(line, HistoryEntry.class));
            } catch (IOException | RuntimeException exception) {
                // a crash between write and force can tear the final line; losing that one dedup
                // entry is benign (the file may be re-imported once), mid-file corruption is not
                if (i == lines.size() - 1) {
                    LOG.warn("Ignoring torn final watcher history line in {}", file, exception);
                    break;
                }
                throw new IllegalStateException("Unable to read watcher history", exception);
            }
        }
        return entries;
    }

    private void writeEntries(String watcherId, List<HistoryEntry> entries) {
        Path target = historyPath(watcherId);
        Path temporary = target.resolveSibling("." + target.getFileName() + "." + UUID.randomUUID() + ".tmp");
        try {
            byte[] bytes = entries.stream()
                    .map(entry -> {
                        try {
                            return mapper.writeValueAsString(entry) + "\n";
                        } catch (IOException exception) {
                            throw new IllegalStateException(exception);
                        }
                    })
                    .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                    .toString()
                    .getBytes(StandardCharsets.UTF_8);
            try (FileChannel channel =
                    FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            java.nio.file.Files.move(
                    temporary,
                    target,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException | RuntimeException exception) {
            throw new IllegalStateException("Unable to compact watcher history", exception);
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException ignored) {
                // The next initialization removes only matching temporary files.
            }
        }
    }

    private Path historyPath(String watcherId) {
        validateWatcherId(watcherId);
        return directory.resolve(watcherId + ".jsonl");
    }

    private void cleanupTemporaryFiles() throws IOException {
        try (var paths = Files.list(directory)) {
            paths.filter(path -> path.getFileName().toString().startsWith(".")
                            && path.getFileName().toString().endsWith(".tmp"))
                    .filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException exception) {
                            throw new IllegalStateException(exception);
                        }
                    });
        }
    }

    private static Path normalize(Path path) {
        return Objects.requireNonNull(path, "source").toAbsolutePath().normalize();
    }

    private static void validateWatcherId(String watcherId) {
        if (watcherId == null || !ID.matcher(watcherId).matches() || watcherId.equals(".") || watcherId.equals("..")) {
            throw new IllegalArgumentException("Invalid watcher ID");
        }
    }

    public record HistoryEntry(
            String sourcePath,
            long size,
            long modifiedAtMillis,
            String fileKey,
            boolean actionCompleted,
            Instant recordedAt,
            SourceFingerprint fingerprint) {

        public HistoryEntry(
                String sourcePath,
                long size,
                long modifiedAtMillis,
                String fileKey,
                boolean actionCompleted,
                Instant recordedAt) {
            this(sourcePath, size, modifiedAtMillis, fileKey, actionCompleted, recordedAt, null);
        }

        public HistoryEntry {
            Objects.requireNonNull(sourcePath, "sourcePath");
            if (size < 0 || modifiedAtMillis < 0)
                throw new IllegalArgumentException("history values must be non-negative");
            Objects.requireNonNull(fileKey, "fileKey");
            Objects.requireNonNull(recordedAt, "recordedAt");
        }
    }
}
