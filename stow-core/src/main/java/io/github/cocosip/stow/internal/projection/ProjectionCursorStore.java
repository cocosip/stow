package io.github.cocosip.stow.internal.projection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.cocosip.stow.internal.sqlite.AtomicJsonFile;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class ProjectionCursorStore {

    private final Path root;
    private final Clock clock;
    private final ObjectMapper mapper;
    private final Map<String, Cursor> cache = new ConcurrentHashMap<>();

    public ProjectionCursorStore(Path root, Clock clock) {
        this.root = root.toAbsolutePath().normalize();
        this.clock = clock;
        mapper = new ObjectMapper().registerModule(new JavaTimeModule()).registerModule(new Jdk8Module());
    }

    public ProjectionCursorStore(Path root) {
        this(root, Clock.systemUTC());
    }

    public Cursor load(String tenantId) {
        return cache.computeIfAbsent(tenantId, this::read);
    }

    public synchronized void save(Cursor cursor) {
        if (cursor == null) throw new IllegalArgumentException("cursor must not be null");
        try {
            AtomicJsonFile<CursorDocument> file =
                    new AtomicJsonFile<>(path(cursor.tenantId()), mapper, CursorDocument.class);
            file.write(new CursorDocument(cursor));
            cache.put(cursor.tenantId(), cursor);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to persist projection cursor", exception);
        }
    }

    private Cursor read(String tenantId) {
        try {
            var file = new AtomicJsonFile<>(path(tenantId), mapper, CursorDocument.class);
            return file.read().map(document -> document.toCursor(tenantId)).orElseGet(() -> Cursor.initial(tenantId));
        } catch (IOException | RuntimeException exception) {
            throw new IllegalStateException("Unable to load projection cursor for tenant " + tenantId, exception);
        }
    }

    private Path path(String tenantId) {
        if (tenantId == null || !tenantId.matches("[A-Za-z0-9._-]{1,128}"))
            throw new IllegalArgumentException("tenantId is not a valid identifier");
        return root.resolve(tenantId).resolve("projector.cursor.json");
    }

    public record Cursor(
            String tenantId, long nextOffset, long lastSequenceNumber, UUID lastEventId, Instant updatedAt) {
        public Cursor {
            if (nextOffset < 0 || lastSequenceNumber < 0)
                throw new IllegalArgumentException("cursor offsets must be non-negative");
            if (tenantId == null || tenantId.isBlank())
                throw new IllegalArgumentException("tenantId must not be blank");
            if (updatedAt == null) throw new IllegalArgumentException("updatedAt must not be null");
        }

        public static Cursor initial(String tenantId) {
            return new Cursor(tenantId, 0, 0, null, Instant.EPOCH);
        }
    }

    private record CursorDocument(
            @JsonProperty("schemaVersion") int schemaVersion,
            @JsonProperty("tenantId") String tenantId,
            @JsonProperty("nextOffset") long nextOffset,
            @JsonProperty("lastSequenceNumber") long lastSequenceNumber,
            @JsonProperty("lastEventId") UUID lastEventId,
            @JsonProperty("updatedAt") Instant updatedAt) {
        @JsonCreator
        CursorDocument {}

        CursorDocument(Cursor cursor) {
            this(
                    1,
                    cursor.tenantId(),
                    cursor.nextOffset(),
                    cursor.lastSequenceNumber(),
                    cursor.lastEventId(),
                    cursor.updatedAt());
        }

        Cursor toCursor(String expectedTenant) {
            if (schemaVersion != 1 || !expectedTenant.equals(tenantId))
                throw new IllegalStateException("Unsupported projection cursor");
            return new Cursor(tenantId, nextOffset, lastSequenceNumber, lastEventId, updatedAt);
        }
    }
}
