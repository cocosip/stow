package io.github.cocosip.stow.internal.scheduler;

import io.github.cocosip.stow.internal.quota.QuotaReservation;
import io.github.cocosip.stow.internal.quota.SqliteQuotaRepository;
import io.github.cocosip.stow.spi.StorageVolume;
import java.nio.file.Path;

final class WriteCompensation {

    private final SqliteQuotaRepository quota;
    private final String tenantId;
    private final QuotaReservation reservation;
    private StorageVolume volume;
    private Path temporary;
    private Path finalPath;
    private boolean publishAttempted;

    WriteCompensation(SqliteQuotaRepository quota, String tenantId, QuotaReservation reservation) {
        this.quota = quota;
        this.tenantId = tenantId;
        this.reservation = reservation;
    }

    void selected(StorageVolume volume, Path temporary, Path finalPath) {
        this.volume = volume;
        this.temporary = temporary;
        this.finalPath = finalPath;
    }

    void cleanupBeforePublish() {
        if (volume != null) {
            delete(temporary);
            delete(finalPath);
        }
        quota.rollback(tenantId, reservation.reservationId());
    }

    void cleanupCandidate() {
        if (volume != null) {
            delete(temporary);
            if (!publishAttempted) delete(finalPath);
        }
    }

    void beforePublish() {
        publishAttempted = true;
    }

    boolean publishAttempted() {
        return publishAttempted;
    }

    void cleanupAfterFailure() {
        // A published file is an orphan and must retain its reservation for recovery.
        if (volume != null) delete(temporary);
    }

    private void delete(Path path) {
        if (path == null) return;
        try {
            volume.delete(path);
        } catch (RuntimeException ignored) {
            // Orphan recovery owns files that cannot be removed synchronously.
        }
    }
}
