package io.github.cocosip.stow.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.cocosip.stow.config.StowConfiguration;
import io.github.cocosip.stow.exception.InvalidConfigurationException;
import java.nio.file.Path;
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
