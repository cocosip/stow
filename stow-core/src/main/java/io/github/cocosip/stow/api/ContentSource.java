package io.github.cocosip.stow.api;

import java.io.InputStream;
import java.util.OptionalLong;

public interface ContentSource {

    InputStream openStream();

    OptionalLong length();

    boolean repeatable();
}
