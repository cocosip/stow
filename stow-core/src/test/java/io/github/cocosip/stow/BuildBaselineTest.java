package io.github.cocosip.stow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class BuildBaselineTest {

    @Test
    void runsOnJava21() {
        assertThat(Runtime.version().feature()).isEqualTo(21);
    }

    @Test
    void loadsSlf4jApiWithoutBundledBinding() {
        assertThat(LoggerFactory.getILoggerFactory()).isNotNull();
        assertThatThrownBy(() -> Class.forName("org.slf4j.impl.StaticLoggerBinder"))
                .isInstanceOf(ClassNotFoundException.class);
    }
}
