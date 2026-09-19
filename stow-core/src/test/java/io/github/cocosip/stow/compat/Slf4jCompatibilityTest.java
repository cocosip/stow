package io.github.cocosip.stow.compat;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.NOPLoggerFactory;

/** Confirms that the test runtime supplies a real SLF4J provider in both profiles. */
class Slf4jCompatibilityTest {

    @Test
    void loadsAProviderInsteadOfTheNopFallback() {
        assertThat(LoggerFactory.getILoggerFactory()).isNotInstanceOf(NOPLoggerFactory.class);
    }
}
