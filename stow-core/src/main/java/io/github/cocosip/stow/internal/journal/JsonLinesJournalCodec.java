package io.github.cocosip.stow.internal.journal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.exception.JournalCorruptionException;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.JournalCodec;
import java.util.zip.CRC32;

public final class JsonLinesJournalCodec implements JournalCodec {

    @Override
    public JournalFormat format() {
        return JournalFormat.JSON_LINES_V1;
    }

    @Override
    public byte[] encode(QueueEventRecord event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        ObjectNode node = QueueEventJson.toNode(event);
        CRC32 crc = new CRC32();
        byte[] canonical = QueueEventJson.encode(event);
        crc.update(canonical);
        node.put("payloadCrc32", crc.getValue());
        try {
            byte[] json = QueueEventJson.MAPPER.writeValueAsBytes(node);
            byte[] result = new byte[json.length + 1];
            System.arraycopy(json, 0, result, 0, json.length);
            result[json.length] = '\n';
            if (json.length > JournalFrame.MAX_PAYLOAD_LENGTH) {
                throw new IllegalArgumentException("journal payload must not exceed 1 MiB");
            }
            return result;
        } catch (Exception exception) {
            if (exception instanceof IllegalArgumentException illegal) {
                throw illegal;
            }
            throw new IllegalArgumentException("Unable to serialize queue event", exception);
        }
    }

    @Override
    public QueueEventRecord decode(byte[] payload) {
        if (payload == null || payload.length == 0 || payload[payload.length - 1] != '\n') {
            throw new JournalCorruptionException("JSON journal record must end with LF");
        }
        for (int i = 0; i < payload.length - 1; i++) {
            if (payload[i] == '\n' || payload[i] == '\r') {
                throw new JournalCorruptionException("JSON journal record contains trailing bytes");
            }
        }
        byte[] json = java.util.Arrays.copyOf(payload, payload.length - 1);
        if (json.length > JournalFrame.MAX_PAYLOAD_LENGTH) {
            throw new JournalCorruptionException("JSON journal payload exceeds 1 MiB");
        }
        JsonNode node = QueueEventJson.parse(json);
        JsonNode crcNode = node.get("payloadCrc32");
        if (crcNode == null
                || !crcNode.isIntegralNumber()
                || crcNode.longValue() < 0
                || crcNode.longValue() > 0xffffffffL) {
            throw new JournalCorruptionException("Missing or invalid JSON journal payload CRC");
        }
        CRC32 crc = new CRC32();
        byte[] canonical = QueueEventJson.canonicalWithoutCrc(node);
        crc.update(canonical);
        if (crc.getValue() != crcNode.longValue()) {
            throw new JournalCorruptionException("JSON journal payload CRC mismatch");
        }
        ObjectNode withoutCrc = (ObjectNode) node.deepCopy();
        withoutCrc.remove("payloadCrc32");
        try {
            if (!java.util.Arrays.equals(canonical, QueueEventJson.MAPPER.writeValueAsBytes(withoutCrc))) {
                throw new JournalCorruptionException("JSON journal record is not canonical");
            }
        } catch (java.io.IOException exception) {
            throw new JournalCorruptionException("Unable to validate canonical JSON", exception);
        }
        return QueueEventJson.fromNode(node);
    }
}
