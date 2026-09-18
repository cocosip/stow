package io.github.cocosip.stow.internal.scheduler;

import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.model.ClaimedFile;
import io.github.cocosip.stow.model.ProcessingLease;
import io.github.cocosip.stow.model.TenantContext;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Narrow scheduler facade used by workers and background services. */
public final class FileScheduler {

    private final StoragePool pool;

    public FileScheduler(StoragePool pool) {
        this.pool = Objects.requireNonNull(pool, "pool");
    }

    public Optional<ClaimedFile> claimNext(TenantContext tenant) {
        return pool.claimNext(tenant);
    }

    public List<ClaimedFile> claimBatch(TenantContext tenant, int batchSize) {
        return pool.claimBatch(tenant, batchSize);
    }

    public void complete(ProcessingLease lease) {
        pool.complete(lease);
    }

    public void fail(ProcessingLease lease, String errorMessage) {
        pool.fail(lease, errorMessage);
    }
}
