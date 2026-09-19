package io.github.cocosip.stow.internal.journal;

import io.github.cocosip.stow.exception.JournalCorruptionException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;

public final class JournalFrame {

    public static final int HEADER_SIZE = 20;
    public static final int TRAILER_SIZE = 4;
    public static final int MIN_FRAME_LENGTH = HEADER_SIZE + TRAILER_SIZE;
    public static final int MAX_PAYLOAD_LENGTH = 1024 * 1024;
    static final byte[] MAGIC = "STW1".getBytes(StandardCharsets.US_ASCII);

    private final int frameLength;
    private final long sequenceNumber;
    private final byte[] payload;
    private final int crc32;

    private JournalFrame(int frameLength, long sequenceNumber, byte[] payload, int crc32) {
        this.frameLength = frameLength;
        this.sequenceNumber = sequenceNumber;
        this.payload = payload;
        this.crc32 = crc32;
    }

    public static byte[] encode(long sequenceNumber, byte[] payload) {
        if (sequenceNumber < 1) {
            throw new IllegalArgumentException("sequenceNumber must be greater than zero");
        }
        if (payload == null || payload.length > MAX_PAYLOAD_LENGTH) {
            throw new IllegalArgumentException("journal payload must not exceed 1 MiB");
        }
        int frameLength = MIN_FRAME_LENGTH + payload.length;
        ByteBuffer frame = ByteBuffer.allocate(frameLength).order(ByteOrder.BIG_ENDIAN);
        frame.put(MAGIC)
                .putInt(frameLength)
                .putLong(sequenceNumber)
                .putInt(payload.length)
                .put(payload);
        CRC32 crc = new CRC32();
        crc.update(frame.array(), 8, frameLength - TRAILER_SIZE - 8);
        frame.putInt((int) crc.getValue());
        return frame.array();
    }

    public static JournalFrame decode(byte[] bytes) {
        if (bytes == null || bytes.length < MIN_FRAME_LENGTH) {
            throw new JournalCorruptionException("Incomplete journal frame");
        }
        if (!Arrays.equals(MAGIC, Arrays.copyOf(bytes, MAGIC.length))) {
            throw new JournalCorruptionException("Invalid journal frame magic");
        }
        ByteBuffer header = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        header.position(4);
        int frameLength = header.getInt();
        long sequenceNumber = header.getLong();
        int payloadLength = header.getInt();
        if (frameLength != bytes.length
                || frameLength < MIN_FRAME_LENGTH
                || payloadLength < 0
                || payloadLength > MAX_PAYLOAD_LENGTH
                || frameLength != MIN_FRAME_LENGTH + payloadLength
                || sequenceNumber < 1) {
            throw new JournalCorruptionException("Invalid journal frame lengths or sequence");
        }
        int expected = ByteBuffer.wrap(bytes, bytes.length - TRAILER_SIZE, TRAILER_SIZE)
                .order(ByteOrder.BIG_ENDIAN)
                .getInt();
        CRC32 crc = new CRC32();
        crc.update(bytes, 8, bytes.length - TRAILER_SIZE - 8);
        if ((int) crc.getValue() != expected) {
            throw new JournalCorruptionException("Journal frame CRC mismatch");
        }
        return new JournalFrame(
                frameLength,
                sequenceNumber,
                Arrays.copyOfRange(bytes, HEADER_SIZE, HEADER_SIZE + payloadLength),
                expected);
    }

    public int frameLength() {
        return frameLength;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public byte[] payload() {
        return payload.clone();
    }

    public int crc32() {
        return crc32;
    }
}
