package io.github.cocosip.stow.internal.watcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.model.PostImportAction;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import io.github.cocosip.stow.model.WatcherRootConfiguration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class FileWatcherAutoManagerTest {

    @Test
    void discoversTenantDirectoriesAndRemovesManagedWatchers() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-auto-");
        Path incoming = root.resolve("incoming");
        Files.createDirectories(incoming.resolve("tenant-a"));
        Files.createDirectories(incoming.resolve("tenant-b"));
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.create(anyString()))
                .thenAnswer(invocation -> new TenantContext(
                        invocation.getArgument(0), TenantStatus.ENABLED, Instant.EPOCH, Instant.EPOCH));
        StoragePool pool = mock(StoragePool.class);
        DefaultFileWatcherManager manager = new DefaultFileWatcherManager(
                root.resolve("state"), pool, tenants, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        DefaultFileWatcherAutoManager auto = new DefaultFileWatcherAutoManager(
                root.resolve("state"), manager, tenants, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        WatcherRootConfiguration configuration = new WatcherRootConfiguration(
                incoming, true, true, true, List.of("**/*.dcm"), PostImportAction.KEEP, null);

        assertThat(auto.apply(configuration)).isEqualTo(2);
        // list() orders by watcherId whose hash now includes the tenantId, so order is not alphabetical
        assertThat(manager.list()).extracting("tenantId").containsExactlyInAnyOrder("tenant-a", "tenant-b");
        assertThat(auto.currentRoot()).contains(configuration);
        auto.removeManagedWatchers();
        assertThat(manager.list()).isEmpty();
    }
}
