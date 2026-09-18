package io.github.cocosip.stow.internal.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.config.RetryConfiguration;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RetryDelayCalculatorTest {

    @Test
    void appliesExponentialDelayAndCapsAtMaximum() {
        RetryDelayCalculator calculator =
                new RetryDelayCalculator(new RetryConfiguration(3, Duration.ofSeconds(2), true, Duration.ofSeconds(5)));

        assertThat(calculator.delayFor(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(calculator.delayFor(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(calculator.delayFor(3)).isEqualTo(Duration.ofSeconds(5));
        assertThat(calculator.isPermanent(3)).isTrue();
    }
}
