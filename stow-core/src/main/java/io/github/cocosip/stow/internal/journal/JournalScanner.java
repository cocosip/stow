package io.github.cocosip.stow.internal.journal;

import io.github.cocosip.stow.exception.JournalCorruptionException;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.JournalCodec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public final class JournalScanner {

    public Result scan(Path log, JournalCodec codec) throws IOException {
        return scan(log, codec, false);
    }

    public Result scan(Path log, JournalCodec codec, boolean allowCompactedSuffix) throws IOException {
        Files.createDirectories(log.getParent());
        if (!Files.exists(log)) {
            Files.createFile(log);
        }
        byte[] bytes = Files.readAllBytes(log);
        return codec.format() == io.github.cocosip.stow.config.JournalFormat.BINARY_V1
                ? scanBinary(log, bytes, codec, allowCompactedSuffix)
                : scanJson(log, bytes, codec, allowCompactedSuffix);
    }

    private Result scanBinary(Path log, byte[] bytes, JournalCodec codec, boolean allowCompactedSuffix)
            throws IOException {
        List<Record> records = new ArrayList<>();
        int offset = 0;
        boolean repaired = false;
        long corruptOffset = -1;
        long expectedSequence = allowCompactedSuffix ? -1 : 1;
        while (offset < bytes.length) {
            int remaining = bytes.length - offset;
            if (remaining < JournalFrame.MIN_FRAME_LENGTH) {
                corruptOffset = offset;
                truncate(log, offset);
                repaired = true;
                break;
            }
            if (bytes[offset] != 'S'
                    || bytes[offset + 1] != 'T'
                    || bytes[offset + 2] != 'W'
                    || bytes[offset + 3] != '1') {
                throw corruption("Middle binary journal corruption at offset " + offset);
            }
            int length = ByteBuffer.wrap(bytes, offset + 4, 4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            if (length < JournalFrame.MIN_FRAME_LENGTH || length > remaining) {
                corruptOffset = offset;
                truncate(log, offset);
                repaired = true;
                break;
            }
            byte[] frame = java.util.Arrays.copyOfRange(bytes, offset, offset + length);
            try {
                JournalFrame decoded = JournalFrame.decode(frame);
                QueueEventRecord event = codec.decode(frame);
                if (expectedSequence < 0) {
                    expectedSequence = event.sequenceNumber();
                }
                if (event.sequenceNumber() != expectedSequence || decoded.sequenceNumber() != expectedSequence) {
                    throw corruption("Journal sequence gap or regression at offset " + offset);
                }
                records.add(new Record(offset, offset + length, event));
                expectedSequence++;
                offset += length;
            } catch (JournalCorruptionException exception) {
                if (offset + length == bytes.length && exception.getMessage().contains("CRC")) {
                    corruptOffset = offset;
                    truncate(log, offset);
                    repaired = true;
                    break;
                }
                throw exception;
            }
        }
        return new Result(
                List.copyOf(records),
                Files.size(log),
                repaired,
                corruptOffset,
                records.isEmpty() ? 0 : records.get(records.size() - 1).event().sequenceNumber());
    }

    private Result scanJson(Path log, byte[] bytes, JournalCodec codec, boolean allowCompactedSuffix)
            throws IOException {
        List<Record> records = new ArrayList<>();
        int offset = 0;
        long expectedSequence = allowCompactedSuffix ? -1 : 1;
        boolean repaired = false;
        long corruptOffset = -1;
        while (offset < bytes.length) {
            int lf = -1;
            for (int i = offset; i < bytes.length; i++) {
                if (bytes[i] == '\n') {
                    lf = i;
                    break;
                }
            }
            if (lf < 0) {
                truncate(log, offset);
                repaired = true;
                corruptOffset = offset;
                break;
            }
            int end = lf + 1;
            byte[] line = java.util.Arrays.copyOfRange(bytes, offset, end);
            try {
                QueueEventRecord event = codec.decode(line);
                if (expectedSequence < 0) {
                    expectedSequence = event.sequenceNumber();
                }
                if (event.sequenceNumber() != expectedSequence) {
                    throw corruption("Journal sequence gap or regression at offset " + offset);
                }
                records.add(new Record(offset, end, event));
                expectedSequence++;
                offset = end;
            } catch (JournalCorruptionException exception) {
                String message = exception.getMessage() == null ? "" : exception.getMessage();
                if (end == bytes.length
                        && !message.contains("Unsupported")
                        && !message.contains("Unknown")
                        && !message.contains("sequence")
                        && !message.contains("Sequence")) {
                    truncate(log, offset);
                    repaired = true;
                    corruptOffset = offset;
                    break;
                }
                throw exception;
            }
        }
        return new Result(
                List.copyOf(records),
                Files.size(log),
                repaired,
                corruptOffset,
                records.isEmpty() ? 0 : records.get(records.size() - 1).event().sequenceNumber());
    }

    private static void truncate(Path log, long size) throws IOException {
        try (var channel = java.nio.channels.FileChannel.open(log, StandardOpenOption.WRITE)) {
            channel.truncate(size);
        }
    }

    private static JournalCorruptionException corruption(String message) {
        return new JournalCorruptionException(message);
    }

    public record Record(long offset, long nextOffset, QueueEventRecord event) {}

    public record Result(
            List<Record> records, long physicalLength, boolean repaired, long corruptOffset, long lastSequenceNumber) {}
}
