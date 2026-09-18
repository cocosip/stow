package io.github.cocosip.stow.internal.journal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.cocosip.stow.exception.JournalCorruptionException;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Set;
import java.util.UUID;

final class QueueEventJson {

    private static final Set<String> FIELD_NAMES = Set.of(
            "schemaVersion",
            "eventId",
            "tenantId",
            "fileKey",
            "eventType",
            "occurredAt",
            "sequenceNumber",
            "volumeId",
            "physicalPath",
            "logicalDirectory",
            "fileSize",
            "status",
            "leaseId",
            "processingStartedAt",
            "retryCount",
            "availableAt",
            "errorMessage",
            "originalFileName",
            "fileExtension");

    static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final DateTimeFormatter TIMESTAMP = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 3, 3, true)
            .appendLiteral('Z')
            .toFormatter()
            .withZone(ZoneOffset.UTC);

    private QueueEventJson() {}

    static byte[] encode(QueueEventRecord event) {
        try {
            return MAPPER.writeValueAsBytes(toNode(event));
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Unable to serialize queue event", exception);
        }
    }

    static QueueEventRecord decode(byte[] json) {
        try {
            QueueEventRecord event = fromNode(MAPPER.readTree(json));
            if (!java.util.Arrays.equals(json, encode(event))) {
                throw new JournalCorruptionException("Queue event JSON is not canonical");
            }
            return event;
        } catch (IOException | IllegalArgumentException exception) {
            if (exception instanceof JournalCorruptionException corruption) {
                throw corruption;
            }
            throw new JournalCorruptionException("Invalid queue event JSON", exception);
        }
    }

    static ObjectNode toNode(QueueEventRecord event) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("schemaVersion", event.schemaVersion());
        node.put("eventId", event.eventId().toString());
        node.put("tenantId", event.tenantId());
        node.put("fileKey", event.fileKey());
        node.put("eventType", event.eventType().name());
        node.put("occurredAt", TIMESTAMP.format(event.occurredAt()));
        node.put("sequenceNumber", event.sequenceNumber());
        node.put("volumeId", event.volumeId());
        node.put("physicalPath", event.physicalPath().toString().replace('\\', '/'));
        node.put("logicalDirectory", event.logicalDirectory());
        node.put("fileSize", event.fileSize());
        node.put("status", event.status().name());
        putNullable(
                node,
                "leaseId",
                event.leaseId() == null ? null : event.leaseId().toString());
        putNullable(node, "processingStartedAt", formatNullable(event.processingStartedAt()));
        node.put("retryCount", event.retryCount());
        putNullable(node, "availableAt", formatNullable(event.availableAt()));
        putNullable(node, "errorMessage", event.errorMessage());
        putNullable(node, "originalFileName", event.originalFileName());
        putNullable(node, "fileExtension", event.fileExtension());
        return node;
    }

    static QueueEventRecord fromNode(JsonNode node) {
        if (node == null || !node.isObject()) {
            throw new JournalCorruptionException("Queue event JSON must be an object");
        }
        int schemaVersion = requiredInt(node, "schemaVersion");
        if (schemaVersion != 1) {
            throw new JournalCorruptionException("Unsupported queue event schema version: " + schemaVersion);
        }
        node.fieldNames().forEachRemaining(name -> {
            if (!FIELD_NAMES.contains(name) && !name.equals("payloadCrc32")) {
                throw new JournalCorruptionException("Unknown queue event field: " + name);
            }
        });
        try {
            return new QueueEventRecord(
                    schemaVersion,
                    UUID.fromString(requiredText(node, "eventId")),
                    requiredText(node, "tenantId"),
                    requiredText(node, "fileKey"),
                    QueueEventType.valueOf(requiredText(node, "eventType")),
                    parseInstant(requiredText(node, "occurredAt")),
                    requiredLong(node, "sequenceNumber"),
                    requiredText(node, "volumeId"),
                    Path.of(requiredText(node, "physicalPath").replace('\\', '/')),
                    requiredText(node, "logicalDirectory"),
                    requiredLong(node, "fileSize"),
                    FileProcessingStatus.valueOf(requiredText(node, "status")),
                    nullableUuid(node, "leaseId"),
                    nullableInstant(node, "processingStartedAt"),
                    requiredInt(node, "retryCount"),
                    nullableInstant(node, "availableAt"),
                    nullableText(node, "errorMessage"),
                    nullableText(node, "originalFileName"),
                    nullableText(node, "fileExtension"));
        } catch (RuntimeException exception) {
            throw new JournalCorruptionException("Invalid queue event fields", exception);
        }
    }

    static byte[] canonicalWithoutCrc(JsonNode node) {
        QueueEventRecord event = fromNode(node);
        return encode(event);
    }

    static JsonNode parse(byte[] bytes) {
        try {
            return MAPPER.readTree(bytes);
        } catch (IOException exception) {
            throw new JournalCorruptionException("Invalid queue event JSON", exception);
        }
    }

    private static String formatNullable(Instant value) {
        return value == null ? null : TIMESTAMP.format(value);
    }

    private static void putNullable(ObjectNode node, String name, String value) {
        if (value != null) {
            node.put(name, value);
        }
    }

    private static JsonNode required(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || value.isNull()) {
            throw new JournalCorruptionException("Missing queue event field: " + name);
        }
        return value;
    }

    private static String requiredText(JsonNode node, String name) {
        JsonNode value = required(node, name);
        if (!value.isTextual() || value.textValue().isEmpty()) {
            throw new JournalCorruptionException("Invalid queue event field: " + name);
        }
        return value.textValue();
    }

    private static String nullableText(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isTextual()) {
            throw new JournalCorruptionException("Invalid queue event field: " + name);
        }
        return value.textValue();
    }

    private static UUID nullableUuid(JsonNode node, String name) {
        String value = nullableText(node, name);
        return value == null ? null : UUID.fromString(value);
    }

    private static Instant nullableInstant(JsonNode node, String name) {
        String value = nullableText(node, name);
        return value == null ? null : parseInstant(value);
    }

    private static Instant parseInstant(String value) {
        Instant parsed = Instant.parse(value);
        if (!TIMESTAMP.format(parsed).equals(value)) {
            throw new JournalCorruptionException("Queue event timestamp is not canonical UTC milliseconds");
        }
        return parsed;
    }

    private static int requiredInt(JsonNode node, String name) {
        JsonNode value = required(node, name);
        if (!value.isIntegralNumber() || !value.canConvertToInt()) {
            throw new JournalCorruptionException("Invalid integer queue event field: " + name);
        }
        return value.intValue();
    }

    private static long requiredLong(JsonNode node, String name) {
        JsonNode value = required(node, name);
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw new JournalCorruptionException("Invalid long queue event field: " + name);
        }
        return value.longValue();
    }
}
