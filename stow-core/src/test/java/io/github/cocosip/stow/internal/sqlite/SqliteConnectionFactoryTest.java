package io.github.cocosip.stow.internal.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SqliteConnectionFactoryTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void opensIsolatedTenantDatabaseWithDocumentedPragmas() throws SQLException {
        Path root = temporaryDirectory.resolve("metadata");
        SqliteConnectionFactory factory =
                new SqliteConnectionFactory(root, "metadata.db", SqliteConnectionFactory.defaults());

        try (Connection connection = factory.open("tenant-a")) {
            assertThat(pragmaText(connection, "journal_mode")).isEqualToIgnoringCase("wal");
            assertThat(pragmaLong(connection, "synchronous")).isEqualTo(1);
            assertThat(pragmaLong(connection, "cache_size")).isEqualTo(-4_000);
            assertThat(pragmaLong(connection, "busy_timeout")).isEqualTo(5_000);
            assertThat(pragmaLong(connection, "foreign_keys")).isEqualTo(1);
            assertThat(pragmaLong(connection, "temp_store")).isEqualTo(2);
        }

        assertThat(factory.databasePath("tenant-a"))
                .isEqualTo(root.resolve("tenant-a")
                        .resolve("metadata.db")
                        .toAbsolutePath()
                        .normalize())
                .exists();
    }

    @Test
    void rejectsTenantTraversalAndInvalidDatabaseFileNames() {
        Path root = temporaryDirectory.resolve("metadata");
        SqliteConnectionFactory factory =
                new SqliteConnectionFactory(root, "metadata.db", SqliteConnectionFactory.defaults());

        assertThatThrownBy(() -> factory.open("../escape")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SqliteConnectionFactory(root, "../outside.db", SqliteConnectionFactory.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(temporaryDirectory.resolve("escape")).doesNotExist();
    }

    @Test
    void rejectsTenantDirectoryLinkThatEscapesTheMetadataRoot() throws IOException {
        Path root = temporaryDirectory.resolve("metadata");
        Path outside = temporaryDirectory.resolve("outside");
        Files.createDirectories(root);
        Files.createDirectories(outside);
        createSymbolicLinkOrSkip(root.resolve("tenant-a"), outside);
        SqliteConnectionFactory factory =
                new SqliteConnectionFactory(root, "metadata.db", SqliteConnectionFactory.defaults());

        assertThatThrownBy(() -> factory.open("tenant-a")).isInstanceOf(SQLException.class);

        assertThat(outside.resolve("metadata.db")).doesNotExist();
    }

    @Test
    void initializesAndChecksSchemaVersion() throws SQLException {
        SqliteConnectionFactory factory = new SqliteConnectionFactory(
                temporaryDirectory.resolve("schema"), "metadata.db", SqliteConnectionFactory.defaults());
        SqliteSchemaManager manager = new SqliteSchemaManager(1);

        try (Connection connection = factory.open("tenant-a")) {
            manager.ensureSchema(connection, List.of("CREATE TABLE sample (id INTEGER PRIMARY KEY NOT NULL)"));
            assertThat(manager.userVersion(connection)).isEqualTo(1);
        }

        try (Connection connection = factory.open("tenant-a")) {
            manager.ensureSchema(connection, List.of());
            assertThatThrownBy(() -> new SqliteSchemaManager(2).ensureSchema(connection, List.of()))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void rollsBackSchemaAndUserVersionWhenInitializationFails() throws SQLException {
        SqliteConnectionFactory factory = new SqliteConnectionFactory(
                temporaryDirectory.resolve("schema-rollback"), "metadata.db", SqliteConnectionFactory.defaults());
        SqliteSchemaManager manager = new SqliteSchemaManager(1);

        try (Connection connection = factory.open("tenant-a")) {
            assertThatThrownBy(() -> manager.ensureSchema(
                            connection,
                            List.of(
                                    "CREATE TABLE sample (id INTEGER PRIMARY KEY NOT NULL)",
                                    "CREATE TABLE sample (id INTEGER PRIMARY KEY NOT NULL)")))
                    .isInstanceOf(SQLException.class);

            assertThat(manager.userVersion(connection)).isZero();
            try (Statement statement = connection.createStatement();
                    ResultSet result = statement.executeQuery(
                            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sample'")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getLong(1)).isZero();
            }
        }
    }

    @Test
    void drainsPartialChannelWrites() throws IOException {
        PartialWriteChannel channel = new PartialWriteChannel();

        AtomicJsonFile.writeFully(channel, ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));

        assertThat(channel.bytes()).containsExactly(1, 2, 3, 4);
    }

    @Test
    void rejectsZeroProgressChannelWrites() {
        ZeroProgressChannel channel = new ZeroProgressChannel();

        assertThatThrownBy(() -> AtomicJsonFile.writeFully(channel, ByteBuffer.wrap(new byte[] {1})))
                .isInstanceOf(IOException.class);
        assertThat(channel.writeAttempts()).isEqualTo(1);
    }

    @Test
    void preservesOldJsonWhenFailureOccursAfterForceBeforeMove() throws IOException {
        Path target = temporaryDirectory.resolve("atomic").resolve("document.json");
        ObjectMapper mapper = new ObjectMapper();
        AtomicJsonFile<TestDocument> initial = new AtomicJsonFile<>(target, mapper, TestDocument.class);
        initial.write(new TestDocument("old"));

        AtomicJsonFile<TestDocument> failing =
                new AtomicJsonFile<>(target, mapper, TestDocument.class, (temporary, destination) -> {
                    throw new IOException("injected before move");
                });

        assertThatThrownBy(() -> failing.write(new TestDocument("new")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("injected");
        assertThat(initial.read()).contains(new TestDocument("old"));
        try (var files = Files.list(target.getParent())) {
            assertThat(files.filter(path -> path.getFileName().toString().endsWith(".tmp")))
                    .isEmpty();
        }
    }

    private static String pragmaText(Connection connection, String name) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("PRAGMA " + name)) {
            assertThat(result.next()).isTrue();
            return result.getString(1);
        }
    }

    private static long pragmaLong(Connection connection, String name) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("PRAGMA " + name)) {
            assertThat(result.next()).isTrue();
            return result.getLong(1);
        }
    }

    private static void createSymbolicLinkOrSkip(Path link, Path target) throws IOException {
        try {
            Files.createSymbolicLink(link, target.toAbsolutePath());
        } catch (UnsupportedOperationException | IOException exception) {
            Assumptions.abort("Symbolic links are unavailable in this environment: " + exception.getMessage());
        }
    }

    private record TestDocument(String value) {}

    private static final class PartialWriteChannel implements WritableByteChannel {

        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private boolean open = true;

        @Override
        public int write(ByteBuffer source) {
            int written = Math.min(source.remaining(), 1);
            output.write(source.get());
            return written;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }

        byte[] bytes() {
            return output.toByteArray();
        }
    }

    private static final class ZeroProgressChannel implements WritableByteChannel {

        private int writeAttempts;

        @Override
        public int write(ByteBuffer source) {
            writeAttempts++;
            if (writeAttempts == 1) {
                return 0;
            }
            throw new AssertionError("writeFully retried after a zero-progress write");
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {}

        int writeAttempts() {
            return writeAttempts;
        }
    }
}
