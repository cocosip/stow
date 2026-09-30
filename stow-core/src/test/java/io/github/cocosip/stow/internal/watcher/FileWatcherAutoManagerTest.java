package io.github.cocosip.stow.internal.watcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.model.PostImportAction;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import io.github.cocosip.stow.model.WatcherRootConfiguration;
import io.github.cocosip.stow.model.WatcherTenantMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class FileWatcherAutoManagerTest {

    @Test
    void createsOneMultiTenantWatcherPerRoot() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-auto-");
        Path incoming = root.resolve("incoming");
        Files.createDirectories(incoming.resolve("tenant-a"));
        Files.createDirectories(incoming.resolve("tenant-b"));
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.list())
                .thenAnswer(ignored -> List.of(
                        new TenantContext("tenant-a", TenantStatus.ENABLED, Instant.EPOCH, Instant.EPOCH),
                        new TenantContext("tenant-b", TenantStatus.ENABLED, Instant.EPOCH, Instant.EPOCH)));
        StoragePool pool = mock(StoragePool.class);
        DefaultFileWatcherManager manager = new DefaultFileWatcherManager(
                root.resolve("state"), pool, tenants, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        DefaultFileWatcherAutoManager auto = new DefaultFileWatcherAutoManager(
                root.resolve("state"), manager, tenants, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        WatcherRootConfiguration configuration = new WatcherRootConfiguration(
                incoming, true, true, true, List.of("**/*.dcm"), PostImportAction.KEEP, null);

        assertThat(auto.apply(configuration)).isEqualTo(1);
        assertThat(manager.list()).hasSize(1);
        assertThat(manager.list().get(0).tenantMode()).isEqualTo(WatcherTenantMode.SUBDIRECTORY_TENANTS);
        assertThat(manager.list().get(0).watchPath())
                .isEqualTo(incoming.toAbsolutePath().normalize());
        assertThat(manager.list().get(0).minimumFileAge()).isEqualTo(java.time.Duration.ofSeconds(5));
        assertThat(manager.list().get(0).stabilityCheckCount()).isEqualTo(2);
        assertThat(auto.currentRoot()).contains(configuration);
        auto.removeManagedWatchers();
        assertThat(manager.list()).isEmpty();
    }

    @Test
    void reapplyingUpdatesTheSameWatcherInsteadOfDuplicating() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-auto-update-");
        Path incoming = root.resolve("incoming");
        Files.createDirectories(incoming);
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.list()).thenAnswer(ignored -> List.of());
        StoragePool pool = mock(StoragePool.class);
        DefaultFileWatcherManager manager = new DefaultFileWatcherManager(
                root.resolve("state"), pool, tenants, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        DefaultFileWatcherAutoManager auto = new DefaultFileWatcherAutoManager(
                root.resolve("state"), manager, tenants, Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
        WatcherRootConfiguration configuration = new WatcherRootConfiguration(
                incoming, true, false, true, List.of("**/*.dcm"), PostImportAction.KEEP, null);

        auto.apply(configuration);
        auto.apply(configuration);

        assertThat(manager.list()).hasSize(1);
    }
}
