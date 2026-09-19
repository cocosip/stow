package io.github.cocosip.stow.spring;

import io.github.cocosip.stow.Stow;
import io.github.cocosip.stow.StowRuntime;
import io.github.cocosip.stow.api.DirectoryQuotaManager;
import io.github.cocosip.stow.api.FileWatcherAutoManager;
import io.github.cocosip.stow.api.FileWatcherManager;
import io.github.cocosip.stow.api.FileWatcherOptionsManager;
import io.github.cocosip.stow.api.QueueProjectionMaintenance;
import io.github.cocosip.stow.api.StatisticsReader;
import io.github.cocosip.stow.api.StorageMaintenance;
import io.github.cocosip.stow.api.StoragePool;
import io.github.cocosip.stow.api.TenantManager;
import io.github.cocosip.stow.api.TenantQuotaManager;
import java.time.Clock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

@AutoConfiguration
@EnableConfigurationProperties(StowProperties.class)
@Import({StowAutoConfiguration.ActuatorConfiguration.class, StowAutoConfiguration.MetricsConfiguration.class})
public class StowAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(Clock.class)
    Clock stowClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean(StowRuntime.class)
    StowRuntime stowRuntime(StowProperties properties, Clock clock) {
        return Stow.builder()
                .configuration(properties.toConfiguration())
                .clock(clock)
                .build();
    }

    @Bean
    StowLifecycle stowLifecycle(StowRuntime runtime) {
        return new StowLifecycle(runtime);
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean(StoragePool.class)
    StoragePool stowStoragePool(StowRuntime runtime) {
        return runtime.storagePool();
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean(TenantManager.class)
    TenantManager stowTenantManager(StowRuntime runtime) {
        return runtime.tenantManager();
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean(TenantQuotaManager.class)
    TenantQuotaManager stowTenantQuotaManager(StowRuntime runtime) {
        return runtime.tenantQuotaManager();
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean(DirectoryQuotaManager.class)
    DirectoryQuotaManager stowDirectoryQuotaManager(StowRuntime runtime) {
        return runtime.directoryQuotaManager();
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean(StorageMaintenance.class)
    StorageMaintenance stowStorageMaintenance(StowRuntime runtime) {
        return runtime.maintenance();
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean(QueueProjectionMaintenance.class)
    QueueProjectionMaintenance stowProjectionMaintenance(StowRuntime runtime) {
        return runtime.projectionMaintenance();
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean(FileWatcherManager.class)
    FileWatcherManager stowFileWatcherManager(StowRuntime runtime) {
        return runtime.fileWatcherManager();
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean(FileWatcherOptionsManager.class)
    FileWatcherOptionsManager stowFileWatcherOptionsManager(StowRuntime runtime) {
        return runtime.fileWatcherOptionsManager();
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean(FileWatcherAutoManager.class)
    FileWatcherAutoManager stowFileWatcherAutoManager(StowRuntime runtime) {
        return runtime.fileWatcherAutoManager();
    }

    @Bean
    @Lazy
    @ConditionalOnMissingBean(StatisticsReader.class)
    StatisticsReader stowStatisticsReader(StowRuntime runtime) {
        return runtime.statisticsReader();
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
    @ConditionalOnProperty(prefix = "stow.actuator", name = "enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnProperty(
            prefix = "management.health.stow",
            name = "enabled",
            havingValue = "true",
            matchIfMissing = true)
    static class ActuatorConfiguration {
        @Bean
        @ConditionalOnMissingBean(StowHealthContributor.class)
        StowHealthContributor stowHealthContributor(StowRuntime runtime) {
            return new StowHealthContributor(runtime);
        }

        @Bean(name = "stowHealthIndicator")
        Object stowHealthIndicator(StowHealthContributor contributor) {
            return contributor.asHealthIndicator();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "io.micrometer.core.instrument.MeterRegistry")
    @ConditionalOnProperty(prefix = "stow.metrics", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class MetricsConfiguration {
        @Bean
        @ConditionalOnMissingBean(StowMeterBinder.class)
        StowMeterBinder stowMeterBinder(StowRuntime runtime) {
            return new StowMeterBinder(runtime);
        }
    }
}
