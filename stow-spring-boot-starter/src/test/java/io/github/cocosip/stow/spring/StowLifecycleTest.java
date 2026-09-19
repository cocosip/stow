package io.github.cocosip.stow.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.cocosip.stow.RuntimeState;
import io.github.cocosip.stow.StowRuntime;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StowLifecycleTest {

    @Test
    void startsAndStopsRuntimeExactlyOnce() {
        StowRuntime runtime = Mockito.mock(StowRuntime.class);
        Mockito.when(runtime.state()).thenReturn(RuntimeState.NEW, RuntimeState.RUNNING, RuntimeState.TERMINATED);
        StowLifecycle lifecycle = new StowLifecycle(runtime);

        lifecycle.start();
        lifecycle.stop();
        lifecycle.stop();

        Mockito.verify(runtime).start();
        Mockito.verify(runtime).close();
        assertThat(lifecycle.isRunning()).isFalse();
    }
}
