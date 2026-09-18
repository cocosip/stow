package io.github.cocosip.stow.internal.quota;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.exception.TenantQuotaExceededException;
import io.github.cocosip.stow.internal.sqlite.SqliteConnectionFactory;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QuotaConcurrencyTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void exactlyOneConcurrentWriterWinsTheLastTenantSlot() throws Exception {
        SqliteQuotaRepository repository = new SqliteQuotaRepository(
                temporaryDirectory, SqliteConnectionFactory.defaults(), Clock.systemUTC(), ignored -> 1);
        CountDownLatch ready = new CountDownLatch(16);
        CountDownLatch start = new CountDownLatch(1);

        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Boolean>> attempts = java.util.stream.IntStream.range(0, 16)
                    .mapToObj(index -> (Callable<Boolean>) () -> {
                        ready.countDown();
                        if (!start.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Concurrent reservation start was not released");
                        }
                        try {
                            repository.reserve("tenant-a", "%032x".formatted(index), "/incoming");
                            return true;
                        } catch (TenantQuotaExceededException expected) {
                            return false;
                        }
                    })
                    .toList();
            List<java.util.concurrent.Future<Boolean>> results =
                    attempts.stream().map(executor::submit).toList();
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            List<Boolean> outcomes = new ArrayList<>(results.size());
            for (var result : results) {
                outcomes.add(result.get(10, TimeUnit.SECONDS));
            }
            assertThat(outcomes).containsOnlyOnce(true);
        }

        assertThat(repository.tenantCurrentCount("tenant-a")).isEqualTo(1);
        assertThat(repository.directoryQuota("tenant-a", "/incoming").currentCount())
                .isEqualTo(1);
    }
}
