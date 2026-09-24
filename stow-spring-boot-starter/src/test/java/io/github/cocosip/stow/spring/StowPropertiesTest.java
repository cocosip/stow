package io.github.cocosip.stow.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.exception.InvalidConfigurationException;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class StowPropertiesTest {

    @Test
    void usesCoreDefaultsWhenNoPropertiesAreBound() {
        StowProperties properties = new StowProperties();

        StowConfiguration configuration = properties.toConfiguration();

        assertThat(configuration.paths().metadataDirectory())
                .isEqualTo(Path.of("stow-metadata").toAbsolutePath());
        assertThat(configuration.paths().quotaDirectory())
                .isEqualTo(Path.of("stow-quota").toAbsolutePath());
        assertThat(configuration.tenant().autoCreateTenants()).isFalse();
        assertThat(configuration.metadata().maxQueueSize()).isEqualTo(100_000);
        assertThat(configuration.journal().ackMode().name()).isEqualTo("DURABLE");
        assertThat(configuration.sourceCleanup().enabled()).isTrue();
        assertThat(configuration.sourceCleanup().databasePath())
                .isEqualTo(Path.of("stow-watchers", "source-cleanup.db").toAbsolutePath());
    }

    @Test
    void mapsSourceCleanupProperties() {
        StowProperties properties = new StowProperties();
        properties.getSourceCleanup().setEnabled(false);
        properties.getSourceCleanup().setDatabasePath(Path.of("state", "cleanup.db"));
        properties.getSourceCleanup().setPollInterval(Duration.ofSeconds(7));
        properties.getSourceCleanup().setMaxConcurrentActions(3);
        properties.getSourceCleanup().setMaxActiveJobs(17);
        properties.getSourceCleanup().setTerminalRetention(Duration.ofHours(2));
        properties.getSourceCleanup().setImportReservationTimeout(Duration.ofMinutes(3));
        properties.getSourceCleanup().setDatabaseOptimizationEnabled(false);
        properties.getSourceCleanup().setDatabaseOptimizationInterval(Duration.ofHours(3));
        properties.getSourceCleanup().setTerminalPruneBatchSize(19);

        var configuration = properties.toConfiguration().sourceCleanup();

        assertThat(configuration.enabled()).isFalse();
        assertThat(configuration.databasePath())
                .isEqualTo(Path.of("state", "cleanup.db").toAbsolutePath());
        assertThat(configuration.pollInterval()).isEqualTo(Duration.ofSeconds(7));
        assertThat(configuration.maxConcurrentActions()).isEqualTo(3);
        assertThat(configuration.maxActiveJobs()).isEqualTo(17);
        assertThat(configuration.terminalRetention()).isEqualTo(Duration.ofHours(2));
        assertThat(configuration.importReservationTimeout()).isEqualTo(Duration.ofMinutes(3));
        assertThat(configuration.databaseOptimizationEnabled()).isFalse();
        assertThat(configuration.databaseOptimizationInterval()).isEqualTo(Duration.ofHours(3));
        assertThat(configuration.terminalPruneBatchSize()).isEqualTo(19);
    }

    @Test
    void rejectsInvalidCoreValuesDuringConversion() {
        StowProperties properties = new StowProperties();
        properties.getMetadata().setSoftMergeThresholdPercent(101);

        assertThatThrownBy(properties::toConfiguration)
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("softMergeThresholdPercent");
    }
}
