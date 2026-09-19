package io.github.cocosip.stow.internal.recovery;

import io.github.cocosip.stow.config.SqliteConfiguration;
import io.github.cocosip.stow.internal.projection.QueueEventReducer;
import io.github.cocosip.stow.internal.projection.SqliteMetadataProjectionStore;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.model.DatabaseRebuildResult;
import io.github.cocosip.stow.spi.QueueEventJournal;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.function.ToLongFunction;

public final class DatabaseRecoveryService {
    private final Path metadataRoot;
    private final Path quotaRoot;
    private final QueueEventJournal journal;
    private final SqliteConfiguration sqlite;
    private final Clock clock;
    private final ToLongFunction<String> initialLimit;

    public DatabaseRecoveryService(
            Path metadataRoot,
            Path quotaRoot,
            QueueEventJournal journal,
            SqliteConfiguration sqlite,
            Clock clock,
            ToLongFunction<String> initialLimit) {
        this.metadataRoot = metadataRoot.toAbsolutePath().normalize();
        this.quotaRoot = quotaRoot.toAbsolutePath().normalize();
        this.journal = java.util.Objects.requireNonNull(journal, "journal");
        this.sqlite = sqlite;
        this.clock = clock == null ? Clock.systemUTC() : clock;
        this.initialLimit = initialLimit == null ? ignored -> 0 : initialLimit;
    }

    public DatabaseRebuildResult rebuildMetadata(String tenantId) {
        Instant started = clock.instant();
        long scanned = 0;
        backup(metadataRoot.resolve(tenantId).resolve("metadata.db"));
        SqliteMetadataProjectionStore metadata = new SqliteMetadataProjectionStore(metadataRoot, sqlite, clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(quotaRoot, sqlite, clock, initialLimit);
        QueueEventReducer reducer = new QueueEventReducer(metadata, quota);
        long offset = journal.baseOffset(tenantId), tail = journal.tailOffset(tenantId);
        while (offset < tail) {
            var batch = journal.readBatch(tenantId, offset, 256);
            if (batch.events().isEmpty() || batch.nextOffset() <= offset) break;
            for (var event : batch.events()) {
                reducer.applyMetadataOnly(event);
                scanned++;
            }
            offset = batch.nextOffset();
        }
        Instant finished = clock.instant();
        return new DatabaseRebuildResult(tenantId, started, finished, scanned, scanned, 0, 0, new ArrayList<>());
    }

    public DatabaseRebuildResult rebuildQuota(String tenantId) {
        Instant started = clock.instant();
        SqliteMetadataProjectionStore metadata = new SqliteMetadataProjectionStore(metadataRoot, sqlite, clock);
        SqliteQuotaRepository quota = new SqliteQuotaRepository(quotaRoot, sqlite, clock, initialLimit);
        var files = metadata.activeFiles(tenantId);
        backup(quotaRoot.resolve(tenantId).resolve("quotas.db"));
        quota.rebuildFromMetadata(tenantId, files);
        return new DatabaseRebuildResult(
                tenantId, started, clock.instant(), files.size(), files.size(), 0, 0, new ArrayList<>());
    }

    public static void rebuildQuotaFromMetadata(
            SqliteQuotaRepository quota, SqliteMetadataProjectionStore metadata, String tenantId) {
        quota.rebuildFromMetadata(tenantId, metadata.activeFiles(tenantId));
    }

    private static void backup(Path database) {
        if (!Files.exists(database)) return;
        try {
            Path backup =
                    database.resolveSibling(database.getFileName() + ".corrupt." + System.currentTimeMillis() + ".bak");
            Files.move(database, backup, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to preserve corrupted database", exception);
        }
    }
}
