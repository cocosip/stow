package io.github.cocosip.stow.internal.projection;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ActiveFileCache {

    private final SqliteMetadataProjectionStore metadata;
    private final Map<String, List<SqliteMetadataProjectionStore.FileRow>> cache = new ConcurrentHashMap<>();

    public ActiveFileCache(SqliteMetadataProjectionStore metadata) {
        this.metadata = metadata;
    }

    public List<SqliteMetadataProjectionStore.FileRow> load(String tenantId) {
        return cache.computeIfAbsent(tenantId, id -> List.copyOf(metadata.activeFiles(id)));
    }

    public List<SqliteMetadataProjectionStore.FileRow> get(String tenantId) {
        return load(tenantId);
    }

    public void invalidate(String tenantId) {
        cache.remove(tenantId);
    }

    public void invalidateAll() {
        cache.clear();
    }
}
