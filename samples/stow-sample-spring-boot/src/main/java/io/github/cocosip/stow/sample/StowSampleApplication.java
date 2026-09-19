package io.github.cocosip.stow.sample;

import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.model.ClaimedFile;
import io.github.cocosip.stow.model.TenantContext;
import io.github.cocosip.stow.model.WriteOptions;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/** Spring Boot entry point using the public services supplied by the Stow starter. */
@SpringBootApplication
public class StowSampleApplication {

    /** Creates the Spring Boot application configuration. */
    public StowSampleApplication() {}

    /**
     * Starts the Spring Boot sample application.
     *
     * @param args Spring Boot command-line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(StowSampleApplication.class, args);
    }

    @Bean
    CommandLineRunner sampleOperation(StowRuntime runtime) {
        return args -> {
            var storagePool = runtime.storagePool();
            var tenantManager = runtime.tenantManager();
            TenantContext tenant = tenantManager.get("sample");
            String fileKey = storagePool.write(
                    tenant,
                    new ByteArrayInputStream("hello from Spring Boot".getBytes(StandardCharsets.UTF_8)),
                    WriteOptions.ofOriginalFileName("hello.txt"));
            ClaimedFile claimed = storagePool.claimNext(tenant).orElseThrow();
            storagePool.complete(claimed.lease());
            System.out.printf("stored %s (%s)%n", fileKey, storagePool.status(tenant, fileKey));
            try (InputStream input = storagePool.read(tenant, fileKey)) {
                System.out.printf("read back: %s%n", new String(input.readAllBytes(), StandardCharsets.UTF_8));
            }
        };
    }
}
