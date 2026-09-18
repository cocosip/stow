package io.github.cocosip.stow.internal.filesystem;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PathPolicyTest {

    @Test
    void rejectsTraversalAndReservedIdentifiersBeforePathResolution() {
        assertThatThrownBy(() -> PathPolicy.identifier("tenantId", "../outside"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PathPolicy.identifier("tenantId", "tenant\\outside"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PathPolicy.identifier("tenantId", "CON")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PathPolicy.identifier("tenantId", "con.txt"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PathPolicy.identifier("tenantId", "lpt9"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsPortableTenantAndFileKeyIdentifiers() {
        assertThat(PathPolicy.identifier("tenantId", "tenant.a_1-2")).isEqualTo("tenant.a_1-2");
        assertThat(PathPolicy.fileKey("0123456789abcdef0123456789abcdef"))
                .isEqualTo("0123456789abcdef0123456789abcdef");
    }

    @Test
    void extractsOnlySafeExtensionsFromOriginalFileNames() {
        assertThat(PathPolicy.extensionFrom("report.final.PDF")).isEqualTo(".PDF");
        assertThat(PathPolicy.extensionFrom("archive.tar.gz")).isEqualTo(".gz");
        assertThat(PathPolicy.extensionFrom("unsafe.<bad>")).isEmpty();
        assertThat(PathPolicy.extensionFrom("too-long." + "x".repeat(32))).isEmpty();
        assertThat(PathPolicy.extensionFrom("no-extension")).isEmpty();
        assertThat(PathPolicy.extension(".txt")).isEqualTo(".txt");
        assertThat(PathPolicy.extension(".unsafe?")).isEmpty();
    }
}
