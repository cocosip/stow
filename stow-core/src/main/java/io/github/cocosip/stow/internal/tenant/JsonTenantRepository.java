package io.github.cocosip.stow.internal.tenant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.cocosip.stow.exception.DatabaseRecoveryException;
import io.github.cocosip.stow.exception.StowInterruptedException;
import io.github.cocosip.stow.internal.sqlite.AtomicJsonFile;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

public final class JsonTenantRepository {

    private static final ConcurrentHashMap<Path, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    private final AtomicJsonFile<TenantDocument> documentFile;
    private final ReentrantLock lock;

    public JsonTenantRepository(Path metadataDirectory) {
        Path documentPath = metadataDirectory.toAbsolutePath().normalize().resolve("tenants.json");
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        documentFile = new AtomicJsonFile<>(documentPath, mapper, TenantDocument.class);
        lock = LOCKS.computeIfAbsent(documentPath, ignored -> new ReentrantLock());
        try {
            documentFile.deleteStaleTemporaryFiles();
        } catch (IOException exception) {
            throw new DatabaseRecoveryException("Unable to remove stale tenants.json temporary files", exception);
        }
    }

    public TenantDocument read() {
        acquireLockInterruptibly();
        try {
            return readUnlocked();
        } finally {
            lock.unlock();
        }
    }

    TenantDocument update(UnaryOperator<TenantDocument> mutation) {
        acquireLockInterruptibly();
        try {
            TenantDocument current = readUnlocked();
            TenantDocument updated = mutation.apply(current);
            if (!updated.equals(current)) {
                documentFile.write(updated);
            }
            return updated;
        } catch (IOException exception) {
            throw new DatabaseRecoveryException("Unable to persist tenants.json", exception);
        } finally {
            lock.unlock();
        }
    }

    private TenantDocument readUnlocked() {
        try {
            return documentFile.read().orElseGet(TenantDocument::empty);
        } catch (IOException exception) {
            throw new DatabaseRecoveryException("Unable to read tenants.json", exception);
        }
    }

    private void acquireLockInterruptibly() {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException exception) {
            throw new StowInterruptedException("Interrupted while waiting for the tenant repository lock", exception);
        }
    }
}
