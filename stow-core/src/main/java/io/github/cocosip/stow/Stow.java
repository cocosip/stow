package io.github.cocosip.stow;

import io.github.cocosip.stow.config.StowConfiguration;

public final class Stow {

    private Stow() {}

    public static StowBuilder builder() {
        return new StowBuilder();
    }

    public static StowRuntime open(StowConfiguration configuration) {
        StowRuntime runtime = builder().configuration(configuration).build();
        runtime.start();
        return runtime;
    }
}
