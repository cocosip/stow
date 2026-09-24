package io.github.cocosip.stow.api;

import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.WriteOptions;

/** Optional storage capability for deduplicating retried writes by a stable operation ID. */
public interface IdempotentStoragePool {

    String writeIdempotently(TenantContext tenant, ContentSource content, WriteOptions options, String operationId);
}
