package io.github.cocosip.stow.internal.watcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.cocosip.stow.api.ContentSource;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.model.PostImportAction;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.TenantStatus;
import io.github.cocosip.stow.model.WatcherConfiguration;
import io.github.cocosip.stow.model.WatcherScanResult;
import io.github.cocosip.stow.model.WatcherTenantMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class WatcherScannerTest {

    private static final Clock CLOCK = Clock.systemUTC();

    @Test
    void filtersGlobRecursionAndSizeAndAppliesDeleteMoveAndKeep() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-scan-");
        Path input = root.resolve("input");
        Path nested = input.resolve("nested");
        Files.createDirectories(nested);
        Path top = input.resolve("top.dcm");
        Path deep = nested.resolve("deep.dcm");
        Path tooLarge = input.resolve("large.dcm");
        Files.writeString(top, "top");
        Files.writeString(deep, "deep");
        Files.writeString(tooLarge, "0123456789");
        StoragePool pool = mock(StoragePool.class);
        when(pool.write(any(TenantContext.class), any(ContentSource.class), any()))
                .thenReturn("0123456789abcdef0123456789abcdef");
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.find("tenant-a")).thenReturn(Optional.of(context("tenant-a")));
        ImportedFileHistory history = new ImportedFileHistory(root.resolve("history"), CLOCK);
        WatcherScanner scanner = new WatcherScanner(pool, tenants, history, CLOCK);

        WatcherConfiguration delete = configuration(input, PostImportAction.DELETE, null, true, 5);
        WatcherScanResult deleted = scanner.scan(delete);
        assertThat(deleted.discoveredCount()).isEqualTo(2);
        assertThat(deleted.importedCount()).isEqualTo(2);
        assertThat(top).doesNotExist();
        assertThat(deep).doesNotExist();
        assertThat(tooLarge).exists();

        Path kept = input.resolve("kept.dcm");
        Files.writeString(kept, "keep");
        WatcherConfiguration keep = configuration(input, PostImportAction.KEEP, null, false, 5);
        WatcherScanResult keptResult = scanner.scan(keep);
        assertThat(keptResult.importedCount()).isEqualTo(1);
        assertThat(kept).exists();

        Path moved = input.resolve("moved.dcm");
        Files.writeString(moved, "move");
        Path destination = root.resolve("done");
        WatcherScanResult movedResult =
                scanner.scan(configuration(input, PostImportAction.MOVE, destination, false, 5));
        assertThat(movedResult.importedCount()).isEqualTo(1);
        assertThat(destination.resolve("moved.dcm")).exists();
    }

    @Test
    void retriesOnlyPostActionAfterHistoryWasWritten() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-retry-");
        Path input = root.resolve("input");
        Files.createDirectories(input);
        Path source = input.resolve("retry.dcm");
        Files.writeString(source, "retry");
        Path invalidDestination = root.resolve("not-a-directory");
        Files.writeString(invalidDestination, "block");
        StoragePool pool = mock(StoragePool.class);
        when(pool.write(any(TenantContext.class), any(ContentSource.class), any()))
                .thenReturn("fedcba9876543210fedcba9876543210");
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.find("tenant-a")).thenReturn(Optional.of(context("tenant-a")));
        ImportedFileHistory history = new ImportedFileHistory(root.resolve("history"), CLOCK);
        WatcherScanner scanner = new WatcherScanner(pool, tenants, history, CLOCK);
        WatcherConfiguration failedMove = configuration(input, PostImportAction.MOVE, invalidDestination, false, 0);

        WatcherScanResult first = scanner.scan(failedMove);
        assertThat(first.importedCount()).isEqualTo(1);
        assertThat(first.failedCount()).isEqualTo(1);
        Path destination = root.resolve("done");
        WatcherScanResult retried = scanner.scan(configuration(input, PostImportAction.MOVE, destination, false, 0));
        assertThat(retried.importedCount()).isEqualTo(0);
        assertThat(retried.skippedCount()).isEqualTo(1);
        assertThat(destination.resolve("retry.dcm")).exists();
        verify(pool, times(1)).write(any(TenantContext.class), any(ContentSource.class), any());
    }

    @Test
    void supportsSubdirectoryTenantsAndBoundsConcurrentImports() throws Exception {
        Path root = Files.createTempDirectory(Path.of("target"), "watcher-multi-");
        Path input = root.resolve("input");
        Files.createDirectories(input.resolve("tenant-a"));
        Files.createDirectories(input.resolve("tenant-b"));
        Files.writeString(input.resolve("tenant-a").resolve("a.dcm"), "a");
        Files.writeString(input.resolve("tenant-b").resolve("b.dcm"), "b");
        StoragePool pool = mock(StoragePool.class);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        doAnswer(invocation -> {
                    int now = active.incrementAndGet();
                    maximum.accumulateAndGet(now, Math::max);
                    Thread.sleep(25);
                    active.decrementAndGet();
                    return "0123456789abcdef0123456789abcdef";
                })
                .when(pool)
                .write(any(TenantContext.class), any(ContentSource.class), any());
        TenantManager tenants = mock(TenantManager.class);
        when(tenants.find(anyString())).thenAnswer(invocation -> Optional.of(context(invocation.getArgument(0))));
        WatcherScanner scanner =
                new WatcherScanner(pool, tenants, new ImportedFileHistory(root.resolve("history"), CLOCK), CLOCK);
        WatcherConfiguration configuration = new WatcherConfiguration(
                "watcher-multi",
                null,
                WatcherTenantMode.SUBDIRECTORY_TENANTS,
                false,
                input,
                true,
                true,
                List.of("**/*.dcm"),
                PostImportAction.KEEP,
                null,
                Duration.ZERO,
                0,
                Duration.ZERO,
                Duration.ZERO,
                1,
                1,
                Duration.ofDays(1),
                Duration.ZERO);

        assertThat(scanner.scan(configuration).importedCount()).isEqualTo(2);
        assertThat(maximum).hasValue(1);
    }

    private static WatcherConfiguration configuration(
            Path input, PostImportAction action, Path moveDirectory, boolean recursive, long maxFileSize) {
        return new WatcherConfiguration(
                "watcher-a",
                "tenant-a",
                WatcherTenantMode.SINGLE_TENANT,
                false,
                input,
                true,
                recursive,
                List.of("**/*.dcm"),
                action,
                moveDirectory,
                Duration.ZERO,
                maxFileSize,
                Duration.ZERO,
                Duration.ZERO,
                1,
                2,
                Duration.ofDays(1),
                Duration.ZERO);
    }

    private static TenantContext context(String tenantId) {
        return new TenantContext(tenantId, TenantStatus.ENABLED, Instant.EPOCH, Instant.EPOCH);
    }
}
