package io.github.cocosip.stow.internal.journal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.cocosip.stow.config.JournalFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.UUID;

public final class JournalStateStore {

    private final Path path;
    private final ObjectMapper mapper =
            QueueEventJson.MAPPER.copy().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public JournalStateStore(Path tenantDirectory) {
        this.path = tenantDirectory.resolve("queue.state.json");
    }

    public State load() throws IOException {
        if (!Files.exists(path)) {
            return null;
        }
        State state = mapper.readValue(path.toFile(), State.class);
        if (state.schemaVersion() != 1
                || state.baseOffset() < 0
                || state.tailOffset() < state.baseOffset()
                || state.lastSequenceNumber() < 0) {
            throw new IOException("Invalid queue state");
        }
        return state;
    }

    public void write(State state) throws IOException {
        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent == null) throw new IOException("Journal state path has no parent: " + path);
        Files.createDirectories(parent);
        Path temporary = parent.resolve(
                "." + path.getFileName() + "." + UUID.randomUUID().toString().toLowerCase() + ".tmp");
        try {
            byte[] json = mapper.writeValueAsBytes(state);
            byte[] withLf = java.util.Arrays.copyOf(json, json.length + 1);
            withLf[json.length] = '\n';
            Files.write(temporary, withLf, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            try (var channel = java.nio.channels.FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    public record State(
            int schemaVersion,
            JournalFormat format,
            long baseOffset,
            long tailOffset,
            long lastSequenceNumber,
            boolean corruptTailDetected,
            Long lastCorruptTailOffset,
            long repairCount,
            Instant updatedAt) {}
}
