package io.github.cocosip.stow.internal.watcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.model.PostImportAction;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherTenantMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class FileWatcherManagerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-19T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void persistsConfigurationAndSupportsLifecycleOperations() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-manager-");
        TenantManager tenants = mock(TenantManager.class);
        StoragePool pool = mock(StoragePool.class);
        DefaultFileWatcherManager manager = new DefaultFileWatcherManager(root, pool, tenants, CLOCK);
        WatcherConfiguration configuration = configuration(root.resolve("in"), "watcher-a", "tenant-a");

        assertThat(manager.register(configuration)).isEqualTo(configuration);
        assertThat(manager.list()).containsExactly(configuration);
        assertThat(manager.listForTenant("tenant-a")).containsExactly(configuration);

        WatcherConfiguration updated = new WatcherConfiguration(
                "watcher-a",
                "tenant-a",
                WatcherTenantMode.SINGLE_TENANT,
                false,
                root.resolve("changed"),
                false,
                true,
                List.of("**/*.dcm"),
                PostImportAction.KEEP,
                null,
                Duration.ofSeconds(1),
                0,
                Duration.ZERO,
                Duration.ZERO,
                1,
                1,
                Duration.ofDays(1),
                Duration.ZERO);
        assertThat(manager.update(updated)).isEqualTo(updated);
        manager.enable("watcher-a");
        assertThat(manager.find("watcher-a"))
                .get()
                .extracting(WatcherConfiguration::enabled)
                .isEqualTo(true);
        manager.disable("watcher-a");
        assertThat(manager.find("watcher-a"))
                .get()
                .extracting(WatcherConfiguration::enabled)
                .isEqualTo(false);

        DefaultFileWatcherManager reopened = new DefaultFileWatcherManager(root, pool, tenants, CLOCK);
        assertThat(reopened.find("watcher-a")).contains(updated);
        reopened.remove("watcher-a");
        assertThat(reopened.find("watcher-a")).isEmpty();
    }

    @Test
    void mapsSingleAndSubdirectoryTenantsAndPersistsOptions() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-options-");
        TenantManager tenants = mock(TenantManager.class);
        StoragePool pool = mock(StoragePool.class);
        DefaultFileWatcherManager manager = new DefaultFileWatcherManager(root, pool, tenants, CLOCK);

        assertThat(manager.options().get().enabled()).isTrue();
        manager.options().disable();
        DefaultFileWatcherManager reopened = new DefaultFileWatcherManager(root, pool, tenants, CLOCK);
        assertThat(reopened.options().get().enabled()).isFalse();
        manager.register(new WatcherConfiguration(
                "watcher-multi",
                null,
                WatcherTenantMode.SUBDIRECTORY_TENANTS,
                true,
                root.resolve("incoming"),
                true,
                true,
                List.of("**/*.dcm"),
                PostImportAction.KEEP,
                null,
                Duration.ofSeconds(1),
                0,
                Duration.ZERO,
                Duration.ZERO,
                1,
                1,
                Duration.ofDays(1),
                Duration.ZERO));
        assertThat(manager.listForTenant("tenant-a")).isEmpty();
    }

    private static WatcherConfiguration configuration(Path path, String watcherId, String tenantId) {
        return new WatcherConfiguration(
                watcherId,
                tenantId,
                WatcherTenantMode.SINGLE_TENANT,
                false,
                path,
                true,
                true,
                List.of("**/*.dcm"),
                PostImportAction.KEEP,
                null,
                Duration.ofSeconds(1),
                0,
                Duration.ZERO,
                Duration.ZERO,
                1,
                1,
                Duration.ofDays(1),
                Duration.ZERO);
    }
}
