package io.github.cocosip.stow.internal.projection;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32;

/** Durable, deterministic projection snapshots with an integrity checksum. */
public final class ProjectionSnapshotStore {

    private final Path root;
    private final ObjectMapper mapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .addModule(new Jdk8Module())
            .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .defaultPropertyInclusion(
                    JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS))
            .build();

    public ProjectionSnapshotStore(Path root) {
        this.root = root.toAbsolutePath().normalize();
    }

    public Path path(String tenantId) {
        if (tenantId == null || !tenantId.matches("[A-Za-z0-9._-]{1,128}")) {
            throw new IllegalArgumentException("tenantId is not a valid identifier");
        }
        return root.resolve(tenantId).resolve("projection.snapshot.json");
    }

    public synchronized void save(Snapshot snapshot) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot must not be null");
        try {
            Path target = path(snapshot.tenantId());
            Path parent = target.getParent();
            if (parent == null) throw new IOException("Snapshot path has no parent: " + target);
            Files.createDirectories(parent);
            ObjectNode document = mapper.valueToTree(snapshot.withoutChecksum(mapper));
            byte[] canonical = mapper.writeValueAsBytes(document);
            document.put("crc32", crc32(canonical));
            byte[] bytes = mapper.writeValueAsBytes(document);
            Path temporary = target.resolveSibling("." + target.getFileName() + ".tmp");
            Files.write(temporary, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to persist projection snapshot", exception);
        }
    }

    public synchronized Snapshot load(String tenantId) {
        try {
            Path target = path(tenantId);
            if (!Files.exists(target)) return null;
            ObjectNode document = (ObjectNode) mapper.readTree(Files.readAllBytes(target));
            JsonNode checksum = document.remove("crc32");
            if (checksum == null || !checksum.isIntegralNumber())
                throw new IllegalStateException("Snapshot CRC is missing");
            long expected = checksum.longValue();
            long actual = crc32(mapper.writeValueAsBytes(document));
            if (actual != expected) throw new IllegalStateException("Snapshot CRC mismatch");
            List<QueueEventRecord> events = new ArrayList<>();
            for (JsonNode event : document.path("events")) {
                events.add(mapper.treeToValue(event, EventDocument.class).toEvent());
            }
            Snapshot snapshot = new Snapshot(
                    document.path("tenantId").asText(),
                    document.path("baseOffset").asLong(),
                    document.path("nextOffset").asLong(),
                    document.path("lastSequenceNumber").asLong(),
                    events);
            if (!tenantId.equals(snapshot.tenantId())) throw new IllegalStateException("Snapshot tenant mismatch");
            return snapshot;
        } catch (IOException | RuntimeException exception) {
            if (exception instanceof IllegalStateException state) throw state;
            throw new IllegalStateException("Unable to load projection snapshot", exception);
        }
    }

    private static long crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return crc.getValue();
    }

    public record Snapshot(
            String tenantId, long baseOffset, long nextOffset, long lastSequenceNumber, List<QueueEventRecord> events) {
        public Snapshot {
            if (tenantId == null || !tenantId.matches("[A-Za-z0-9._-]{1,128}"))
                throw new IllegalArgumentException("tenantId is not valid");
            if (baseOffset < 0 || nextOffset < baseOffset || lastSequenceNumber < 0)
                throw new IllegalArgumentException("snapshot offsets are invalid");
            events = List.copyOf(events == null ? List.of() : events);
        }

        private ObjectNode withoutChecksum(ObjectMapper mapper) {
            ObjectNode node = mapper.createObjectNode();
            node.put("schemaVersion", 1);
            node.put("tenantId", tenantId);
            node.put("baseOffset", baseOffset);
            node.put("nextOffset", nextOffset);
            node.put("lastSequenceNumber", lastSequenceNumber);
            var array = node.putArray("events");
            for (QueueEventRecord event : events) array.add(mapper.valueToTree(EventDocument.from(event)));
            return node;
        }

        @Override
        public List<QueueEventRecord> events() {
            return events;
        }
    }

    private record EventDocument(
            int schemaVersion,
            UUID eventId,
            String tenantId,
            String fileKey,
            QueueEventType eventType,
            Instant occurredAt,
            long sequenceNumber,
            String volumeId,
            String physicalPath,
            String logicalDirectory,
            long fileSize,
            FileProcessingStatus status,
            UUID leaseId,
            Instant processingStartedAt,
            int retryCount,
            Instant availableAt,
            String errorMessage,
            String originalFileName,
            String fileExtension,
            String importOperationId) {
        static EventDocument from(QueueEventRecord event) {
            return new EventDocument(
                    event.schemaVersion(),
                    event.eventId(),
                    event.tenantId(),
                    event.fileKey(),
                    event.eventType(),
                    event.occurredAt(),
                    event.sequenceNumber(),
                    event.volumeId(),
                    event.physicalPath().toString(),
                    event.logicalDirectory(),
                    event.fileSize(),
                    event.status(),
                    event.leaseId(),
                    event.processingStartedAt(),
                    event.retryCount(),
                    event.availableAt(),
                    event.errorMessage(),
                    event.originalFileName(),
                    event.fileExtension(),
                    event.importOperationId());
        }

        QueueEventRecord toEvent() {
            return new QueueEventRecord(
                    schemaVersion,
                    eventId,
                    tenantId,
                    fileKey,
                    eventType,
                    occurredAt,
                    sequenceNumber,
                    volumeId,
                    Path.of(physicalPath),
                    logicalDirectory,
                    fileSize,
                    status,
                    leaseId,
                    processingStartedAt,
                    retryCount,
                    availableAt,
                    errorMessage,
                    originalFileName,
                    fileExtension,
                    importOperationId);
        }
    }
}
