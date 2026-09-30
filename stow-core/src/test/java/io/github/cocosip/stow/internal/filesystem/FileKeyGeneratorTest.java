package io.github.cocosip.stow.internal.filesystem;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FileKeyGeneratorTest {

    @Test
    void keysMatchTheFileKeyContract() {
        FileKeyGenerator generator = new FileKeyGenerator(42);
        Set<String> keys = new HashSet<>();
        for (int index = 0; index < 1_000; index++) {
            String key = generator.next();
            assertThat(key).matches("[0-9a-f]{32}");
            keys.add(key);
        }
        assertThat(keys).hasSize(1_000);
    }

    @Test
    void shortWriteBurstsShareOneShardDirectory() {
        FileKeyGenerator generator = new FileKeyGenerator(42);
        Set<String> shardPrefixes = new HashSet<>();
        for (int index = 0; index < 32; index++) {
            shardPrefixes.add(generator.next().substring(0, 2));
        }
        // Locus shard locality: one burst of up to 32 keys lands in one first-level shard.
        assertThat(shardPrefixes).hasSize(1);
    }

    @Test
    void distinctBurstsSpreadAcrossShards() {
        FileKeyGenerator generator = new FileKeyGenerator(42);
        Set<String> shardPrefixes = new HashSet<>();
        for (int burst = 0; burst < 50; burst++) {
            shardPrefixes.add(generator.next().substring(0, 2));
            for (int index = 1; index < 32; index++) {
                generator.next();
            }
        }
        assertThat(shardPrefixes.size()).isGreaterThanOrEqualTo(20);
    }
}
