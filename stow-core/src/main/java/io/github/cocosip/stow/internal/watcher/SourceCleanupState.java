package io.github.cocosip.stow.internal.watcher;

enum SourceCleanupState {
    IMPORTING,
    PENDING,
    RETRYING,
    MOVE_PENDING,
    TERMINAL_KEEP,
    TERMINAL_FAILED
}
