package io.github.cocosip.stow.internal.journal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.exception.JournalCorruptionException;
import io.github.cocosip.stow.model.FileProcessingStatus;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.model.QueueEventType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.zip.CRC32;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

class JournalCodecTest {

    private static final UUID EVENT_ID = UUID.fromString("123e4567-e89b-42d3-a456-426614174000");
    private static final QueueEventRecord EVENT = new QueueEventRecord(
            1,
            EVENT_ID,
            "tenant-a",
            "0123456789abcdef0123456789abcdef",
            QueueEventType.PROCESSING_FAILED,
            Instant.parse("2026-09-18T00:00:00.123Z"),
            7,
            "volume-1",
            Path.of("/var/stow/file.bin"),
            "/incoming",
            42,
            FileProcessingStatus.FAILED,
            null,
            null,
            2,
            null,
            "失败: 读取错误",
            null,
            ".bin");

    @Test
    void binaryRoundTripHasContractFrameAndStableFixture() throws Exception {
        BinaryV1JournalCodec codec = new BinaryV1JournalCodec();
        byte[] encoded = codec.encode(EVENT);

        assertThat(new String(encoded, 0, 4, StandardCharsets.US_ASCII)).isEqualTo("STW1");
        assertThat(readInt(encoded, 4)).isEqualTo(encoded.length);
        assertThat(readLong(encoded, 8)).isEqualTo(7);
        assertThat(readInt(encoded, 16)).isEqualTo(encoded.length - 24);
        CRC32 crc = new CRC32();
        crc.update(encoded, 8, encoded.length - 12);
        assertThat(readInt(encoded, encoded.length - 4)).isEqualTo((int) crc.getValue());
        assertThat(codec.decode(encoded)).isEqualTo(EVENT);
    }

    @Test
    void jsonLinesRoundTripUsesLfAndPayloadCrc() {
        JsonLinesJournalCodec codec = new JsonLinesJournalCodec();
        byte[] encoded = codec.encode(EVENT);

        assertThat(encoded[encoded.length - 1]).isEqualTo((byte) '\n');
        assertThat(codec.decode(encoded)).isEqualTo(EVENT);
        assertThat(new String(encoded, StandardCharsets.UTF_8)).contains("\"payloadCrc32\":");
    }

    @Test
    void rejectsPayloadCorruptionAndTrailingBytes() {
        BinaryV1JournalCodec binary = new BinaryV1JournalCodec();
        byte[] bytes = binary.encode(EVENT);
        bytes[bytes.length - 1] ^= 1;
        assertThatThrownBy(() -> binary.decode(bytes)).isInstanceOf(JournalCorruptionException.class);

        JsonLinesJournalCodec json = new JsonLinesJournalCodec();
        byte[] line = json.encode(EVENT);
        byte[] trailing = new byte[line.length + 1];
        System.arraycopy(line, 0, trailing, 0, line.length);
        trailing[trailing.length - 1] = 'x';
        assertThatThrownBy(() -> json.decode(trailing)).isInstanceOf(JournalCorruptionException.class);
    }

    @Test
    void detectorSelectsFormatsAndRejectsUnknownInput() {
        JournalFormatDetector detector = new JournalFormatDetector();
        assertThat(detector.detect(new BinaryV1JournalCodec().encode(EVENT))).isEqualTo(JournalFormat.BINARY_V1);
        assertThat(detector.detect(new JsonLinesJournalCodec().encode(EVENT))).isEqualTo(JournalFormat.JSON_LINES_V1);
        assertThatThrownBy(() -> detector.detect("unknown".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(JournalCorruptionException.class);
    }

    @Test
    void roundTripsEveryEventTypeAndRejectsUnknownSchema() throws Exception {
        BinaryV1JournalCodec codec = new BinaryV1JournalCodec();
        for (QueueEventType type : QueueEventType.values()) {
            QueueEventRecord event = new QueueEventRecord(
                    EVENT.schemaVersion(),
                    EVENT.eventId(),
                    EVENT.tenantId(),
                    EVENT.fileKey(),
                    type,
                    EVENT.occurredAt(),
                    EVENT.sequenceNumber(),
                    EVENT.volumeId(),
                    EVENT.physicalPath(),
                    EVENT.logicalDirectory(),
                    EVENT.fileSize(),
                    EVENT.status(),
                    EVENT.leaseId(),
                    EVENT.processingStartedAt(),
                    EVENT.retryCount(),
                    EVENT.availableAt(),
                    EVENT.errorMessage(),
                    EVENT.originalFileName(),
                    EVENT.fileExtension());
            assertThat(codec.decode(codec.encode(event))).isEqualTo(event);
        }
        byte[] unknownJson = QueueEventJson.encode(EVENT);
        final byte[] unknown = new String(unknownJson, StandardCharsets.UTF_8)
                .replace("\"schemaVersion\":1", "\"schemaVersion\":2")
                .getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> codec.decode(JournalFrame.encode(EVENT.sequenceNumber(), unknown)))
                .isInstanceOf(JournalCorruptionException.class);
    }

    @Test
    void enforcesOneMiBPayloadLimit() {
        String longPath = "/" + "x".repeat(JournalFrame.MAX_PAYLOAD_LENGTH);
        QueueEventRecord oversized = new QueueEventRecord(
                EVENT.schemaVersion(),
                EVENT.eventId(),
                EVENT.tenantId(),
                EVENT.fileKey(),
                EVENT.eventType(),
                EVENT.occurredAt(),
                EVENT.sequenceNumber(),
                EVENT.volumeId(),
                Path.of(longPath),
                EVENT.logicalDirectory(),
                EVENT.fileSize(),
                EVENT.status(),
                EVENT.leaseId(),
                EVENT.processingStartedAt(),
                EVENT.retryCount(),
                EVENT.availableAt(),
                EVENT.errorMessage(),
                EVENT.originalFileName(),
                EVENT.fileExtension());
        assertThatThrownBy(() -> new BinaryV1JournalCodec().encode(oversized))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JsonLinesJournalCodec().encode(oversized))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writesAndReadsGoldenFixtures() throws Exception {
        BinaryV1JournalCodec binary = new BinaryV1JournalCodec();
        JsonLinesJournalCodec json = new JsonLinesJournalCodec();
        byte[] binaryFixture = getResource("/journal/v1/binary-event.bin");
        byte[] jsonFixture = getResource("/journal/v1/json-event.jsonl");
        assertThat(binary.encode(EVENT)).isEqualTo(binaryFixture);
        assertThat(json.encode(EVENT)).isEqualTo(jsonFixture);
        assertThat(binary.decode(binaryFixture)).isEqualTo(EVENT);
        assertThat(json.decode(jsonFixture)).isEqualTo(EVENT);
    }

    @Test
    void rejectsFrameSequenceThatDiffersFromEventSequence() {
        byte[] frame = JournalFrame.encode(99, QueueEventJson.encode(EVENT));
        assertThatThrownBy(() -> new BinaryV1JournalCodec().decode(frame))
                .isInstanceOf(JournalCorruptionException.class);
    }

    @Test
    void rejectsNonIntegralNumericFieldsAndUnknownOrExplicitNullFields() throws Exception {
        JsonLinesJournalCodec codec = new JsonLinesJournalCodec();
        ObjectNode numeric = QueueEventJson.toNode(EVENT);
        numeric.put("schemaVersion", "1");
        assertThatThrownBy(() -> codec.decode(jsonLineWithCrc(numeric)))
                .isInstanceOf(JournalCorruptionException.class);

        ObjectNode unknown = QueueEventJson.toNode(EVENT);
        unknown.put("unexpected", true);
        assertThatThrownBy(() -> codec.decode(jsonLineWithCrc(unknown)))
                .isInstanceOf(JournalCorruptionException.class);

        ObjectNode explicitNull = QueueEventJson.toNode(EVENT);
        explicitNull.putNull("leaseId");
        assertThatThrownBy(() -> codec.decode(jsonLineWithCrc(explicitNull)))
                .isInstanceOf(JournalCorruptionException.class);
    }

    @Test
    void rejectsReorderedJsonEvenWhenItsInputOrderHasAValidCrc() throws Exception {
        ObjectNode reordered = QueueEventJson.toNode(EVENT);
        ObjectNode copy = QueueEventJson.MAPPER.createObjectNode();
        var names = new java.util.ArrayList<String>();
        reordered.fieldNames().forEachRemaining(names::add);
        java.util.Collections.reverse(names);
        names.forEach(name -> copy.set(name, reordered.get(name)));
        byte[] line = jsonLineWithCrc(copy);
        assertThatThrownBy(() -> new JsonLinesJournalCodec().decode(line))
                .isInstanceOf(JournalCorruptionException.class);
    }

    private static byte[] jsonLineWithCrc(ObjectNode node) throws Exception {
        CRC32 crc = new CRC32();
        ObjectNode withoutCrc = (ObjectNode) node.deepCopy();
        withoutCrc.remove("payloadCrc32");
        crc.update(QueueEventJson.MAPPER.writeValueAsBytes(withoutCrc));
        node.put("payloadCrc32", crc.getValue());
        byte[] json = QueueEventJson.MAPPER.writeValueAsBytes(node);
        byte[] line = java.util.Arrays.copyOf(json, json.length + 1);
        line[line.length - 1] = '\n';
        return line;
    }

    private static byte[] getResource(String name) throws Exception {
        try (var stream = JournalCodecTest.class.getResourceAsStream(name)) {
            return stream.readAllBytes();
        }
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24)
                | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8)
                | (bytes[offset + 3] & 0xff);
    }

    private static long readLong(byte[] bytes, int offset) {
        long result = 0;
        for (int index = 0; index < 8; index++) {
            result = (result << 8) | (bytes[offset + index] & 0xffL);
        }
        return result;
    }
}
