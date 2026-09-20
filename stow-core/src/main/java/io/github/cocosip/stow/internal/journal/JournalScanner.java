package io.github.cocosip.stow.internal.journal;

import io.github.cocosip.stow.config.JournalFormat;
import io.github.cocosip.stow.exception.JournalCorruptionException;
import io.github.cocosip.stow.model.QueueEventRecord;
import io.github.cocosip.stow.spi.JournalCodec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public final class JournalScanner {

    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int MAX_BINARY_FRAME_LENGTH = JournalFrame.MIN_FRAME_LENGTH + JournalFrame.MAX_PAYLOAD_LENGTH;
    private static final int MAX_JSON_RECORD_LENGTH = JournalFrame.MAX_PAYLOAD_LENGTH + 1;

    public Result scan(Path log, JournalCodec codec) throws IOException {
        return scan(log, codec, false);
    }

    public Result scan(Path log, JournalCodec codec, boolean allowCompactedSuffix) throws IOException {
        Path parent = log.toAbsolutePath().normalize().getParent();
        if (parent == null) throw new IOException("Journal path has no parent: " + log);
        Files.createDirectories(parent);
        try (FileChannel channel =
                FileChannel.open(log, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            return codec.format() == JournalFormat.BINARY_V1
                    ? scanBinary(channel, codec, allowCompactedSuffix)
                    : scanJson(channel, codec, allowCompactedSuffix);
        }
    }

    public ReadResult readBatch(Path log, JournalCodec codec, long offset, long physicalTail, int maxRecords)
            throws IOException {
        if (offset < 0 || physicalTail < offset || maxRecords <= 0) {
            throw new IllegalArgumentException("invalid journal read bounds");
        }
        try (FileChannel channel = FileChannel.open(log, StandardOpenOption.READ)) {
            long end = Math.min(channel.size(), physicalTail);
            return codec.format() == JournalFormat.BINARY_V1
                    ? readBinaryBatch(channel, codec, offset, end, maxRecords)
                    : readJsonBatch(channel, codec, offset, end, maxRecords);
        }
    }

    private Result scanBinary(FileChannel channel, JournalCodec codec, boolean allowCompactedSuffix)
            throws IOException {
        long length = channel.size();
        long offset = 0;
        long firstSequence = 0;
        long lastSequence = 0;
        long recordCount = 0;
        boolean repaired = false;
        long corruptOffset = -1;
        long expectedSequence = allowCompactedSuffix ? -1 : 1;
        while (offset < length) {
            long remaining = length - offset;
            if (remaining < JournalFrame.MIN_FRAME_LENGTH) {
                corruptOffset = offset;
                channel.truncate(offset);
                length = offset;
                repaired = true;
                break;
            }
            int frameLength = binaryFrameLength(channel, offset);
            if (frameLength < JournalFrame.MIN_FRAME_LENGTH) {
                corruptOffset = offset;
                channel.truncate(offset);
                length = offset;
                repaired = true;
                break;
            }
            if (frameLength > MAX_BINARY_FRAME_LENGTH) {
                throw corruption("Invalid binary journal frame length at offset " + offset);
            }
            if (frameLength > remaining) {
                corruptOffset = offset;
                channel.truncate(offset);
                length = offset;
                repaired = true;
                break;
            }
            byte[] frame = readBytes(channel, offset, frameLength);
            try {
                JournalFrame decoded = JournalFrame.decode(frame);
                QueueEventRecord event = codec.decode(frame);
                if (expectedSequence < 0) expectedSequence = event.sequenceNumber();
                if (event.sequenceNumber() != expectedSequence || decoded.sequenceNumber() != expectedSequence) {
                    throw corruption("Journal sequence gap or regression at offset " + offset);
                }
                if (recordCount == 0) firstSequence = event.sequenceNumber();
                lastSequence = event.sequenceNumber();
                recordCount++;
                expectedSequence++;
                offset += frameLength;
            } catch (JournalCorruptionException exception) {
                if (offset + frameLength == length && message(exception).contains("CRC")) {
                    corruptOffset = offset;
                    channel.truncate(offset);
                    length = offset;
                    repaired = true;
                    break;
                }
                throw exception;
            }
        }
        return new Result(length, repaired, corruptOffset, firstSequence, lastSequence, recordCount);
    }

    private Result scanJson(FileChannel channel, JournalCodec codec, boolean allowCompactedSuffix) throws IOException {
        long length = channel.size();
        long offset = 0;
        long recordStart = 0;
        long firstSequence = 0;
        long lastSequence = 0;
        long recordCount = 0;
        long expectedSequence = allowCompactedSuffix ? -1 : 1;
        boolean repaired = false;
        long corruptOffset = -1;
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        while (offset < length) {
            buffer.clear();
            buffer.limit((int) Math.min(buffer.capacity(), length - offset));
            int read = channel.read(buffer, offset);
            if (read <= 0) throw new IOException("Journal channel made no progress");
            buffer.flip();
            while (buffer.hasRemaining()) {
                byte value = buffer.get();
                offset++;
                line.write(value);
                if (line.size() > MAX_JSON_RECORD_LENGTH) {
                    throw corruption("JSON journal record exceeds 1 MiB at offset " + recordStart);
                }
                if (value != '\n') continue;
                byte[] encoded = line.toByteArray();
                try {
                    QueueEventRecord event = codec.decode(encoded);
                    if (expectedSequence < 0) expectedSequence = event.sequenceNumber();
                    if (event.sequenceNumber() != expectedSequence) {
                        throw corruption("Journal sequence gap or regression at offset " + recordStart);
                    }
                    if (recordCount == 0) firstSequence = event.sequenceNumber();
                    lastSequence = event.sequenceNumber();
                    recordCount++;
                    expectedSequence++;
                    recordStart = offset;
                    line.reset();
                } catch (JournalCorruptionException exception) {
                    if (offset == length && repairableFinalJson(exception)) {
                        corruptOffset = recordStart;
                        channel.truncate(recordStart);
                        length = recordStart;
                        repaired = true;
                        line.reset();
                        break;
                    }
                    throw exception;
                }
            }
        }
        if (line.size() > 0) {
            corruptOffset = recordStart;
            channel.truncate(recordStart);
            length = recordStart;
            repaired = true;
        }
        return new Result(length, repaired, corruptOffset, firstSequence, lastSequence, recordCount);
    }

    private ReadResult readBinaryBatch(FileChannel channel, JournalCodec codec, long offset, long end, int maxRecords)
            throws IOException {
        List<QueueEventRecord> events = new ArrayList<>(Math.min(maxRecords, 256));
        long cursor = offset;
        while (cursor < end && events.size() < maxRecords) {
            if (end - cursor < JournalFrame.MIN_FRAME_LENGTH) {
                throw corruption("Incomplete binary journal frame at offset " + cursor);
            }
            int frameLength = binaryFrameLength(channel, cursor);
            if (frameLength < JournalFrame.MIN_FRAME_LENGTH
                    || frameLength > MAX_BINARY_FRAME_LENGTH
                    || cursor + frameLength > end) {
                throw corruption("Invalid binary journal frame length at offset " + cursor);
            }
            events.add(codec.decode(readBytes(channel, cursor, frameLength)));
            cursor += frameLength;
        }
        long lastSequence = events.isEmpty() ? 0 : events.get(events.size() - 1).sequenceNumber();
        return new ReadResult(events, cursor, lastSequence);
    }

    private ReadResult readJsonBatch(FileChannel channel, JournalCodec codec, long offset, long end, int maxRecords)
            throws IOException {
        List<QueueEventRecord> events = new ArrayList<>(Math.min(maxRecords, 256));
        long cursor = offset;
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        while (cursor < end && events.size() < maxRecords) {
            buffer.clear();
            buffer.limit((int) Math.min(buffer.capacity(), end - cursor));
            int read = channel.read(buffer, cursor);
            if (read <= 0) throw new IOException("Journal channel made no progress");
            buffer.flip();
            while (buffer.hasRemaining() && events.size() < maxRecords) {
                byte value = buffer.get();
                cursor++;
                line.write(value);
                if (line.size() > MAX_JSON_RECORD_LENGTH) {
                    throw corruption("JSON journal record exceeds 1 MiB at offset " + (cursor - line.size()));
                }
                if (value == '\n') {
                    events.add(codec.decode(line.toByteArray()));
                    line.reset();
                }
            }
        }
        if (cursor == end && line.size() > 0) {
            throw corruption("Incomplete JSON journal record at offset " + (cursor - line.size()));
        }
        long lastSequence = events.isEmpty() ? 0 : events.get(events.size() - 1).sequenceNumber();
        return new ReadResult(events, cursor, lastSequence);
    }

    private static int binaryFrameLength(FileChannel channel, long offset) throws IOException {
        byte[] header = readBytes(channel, offset, 8);
        if (header[0] != 'S' || header[1] != 'T' || header[2] != 'W' || header[3] != '1') {
            throw corruption("Middle binary journal corruption at offset " + offset);
        }
        return ByteBuffer.wrap(header, 4, 4).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    private static byte[] readBytes(FileChannel channel, long offset, int length) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        long cursor = offset;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, cursor);
            if (read < 0) throw new IOException("Unexpected end of journal");
            if (read == 0) throw new IOException("Journal channel made no progress");
            cursor += read;
        }
        return buffer.array();
    }

    private static boolean repairableFinalJson(JournalCorruptionException exception) {
        String message = message(exception);
        return !message.contains("Unsupported")
                && !message.contains("Unknown")
                && !message.contains("sequence")
                && !message.contains("Sequence");
    }

    private static String message(Exception exception) {
        return exception.getMessage() == null ? "" : exception.getMessage();
    }

    private static JournalCorruptionException corruption(String message) {
        return new JournalCorruptionException(message);
    }

    public record Result(
            long physicalLength,
            boolean repaired,
            long corruptOffset,
            long firstSequenceNumber,
            long lastSequenceNumber,
            long recordCount) {}

    public record ReadResult(List<QueueEventRecord> events, long nextOffset, long lastSequenceNumber) {
        public ReadResult {
            events = List.copyOf(events);
        }
    }
}
