package io.github.cocosip.stow.sample;

import io.github.cocosip.stow.Stow;
import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.config.VolumeConfiguration;
import io.github.cocosip.stow.model.ClaimedFile;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.WriteOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/** Small framework-neutral application showing the complete write/claim/complete flow. */
public final class ConsoleSampleApplication {

    private ConsoleSampleApplication() {}

    /**
     * Runs the write, claim, complete, close, reopen, and read demonstration.
     *
     * @param args optional root directory for sample data
     */
    public static void main(String[] args) {
        Path root = args.length == 0 ? Path.of("sample-data") : Path.of(args[0]);
        StowConfiguration configuration = StowConfiguration.builder()
                .metadataDirectory(root.resolve("metadata"))
                .quotaDirectory(root.resolve("quota"))
                .queueDirectory(root.resolve("queue"))
                .watcherDirectory(root.resolve("watchers"))
                .autoCreateTenants(true)
                .volumes(List.of(new VolumeConfiguration("primary", root.resolve("volume"), 2, 65_536, true)))
                .build();

        String fileKey;
        try (StowRuntime runtime = Stow.open(configuration)) {
            TenantContext tenant = runtime.tenantManager().get("sample");
            fileKey = runtime.storagePool()
                    .write(
                            tenant,
                            new ByteArrayInputStream("hello from Stow".getBytes(StandardCharsets.UTF_8)),
                            WriteOptions.ofOriginalFileName("hello.txt"));
            ClaimedFile claimed = runtime.storagePool().claimNext(tenant).orElseThrow();
            runtime.storagePool().complete(claimed.lease());
            System.out.printf(
                    "stored %s as %s (%s)%n",
                    fileKey,
                    claimed.location().physicalPath(),
                    runtime.storagePool().status(tenant, fileKey));
            System.out.printf("read back: %s%n", readUtf8(runtime, tenant, fileKey));
        }

        try (StowRuntime reopened = Stow.open(configuration)) {
            TenantContext tenant = reopened.tenantManager().get("sample");
            System.out.printf("reopened read: %s%n", readUtf8(reopened, tenant, fileKey));
        }
    }

    private static String readUtf8(StowRuntime runtime, TenantContext tenant, String fileKey) {
        try (InputStream input = runtime.storagePool().read(tenant, fileKey)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not read stored sample", exception);
        }
    }
}
