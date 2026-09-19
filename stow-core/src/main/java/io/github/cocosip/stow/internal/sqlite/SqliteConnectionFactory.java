package io.github.cocosip.stow.internal.sqlite;

import io.github.cocosip.stow.config.SqliteConfiguration;
import io.github.cocosip.stow.config.SqliteJournalMode;
import io.github.cocosip.stow.config.SqliteSynchronousMode;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

public final class SqliteConnectionFactory {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9._-]{1,128}");
    private static final Set<String> WINDOWS_RESERVED = Set.of(
            "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1",
            "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9");

    private final Path rootDirectory;
    private final String databaseFileName;
    private final SqliteConfiguration configuration;
    private final BeforeTenantDirectoryCreate beforeTenantDirectoryCreate;

    public SqliteConnectionFactory(Path rootDirectory, String databaseFileName, SqliteConfiguration configuration) {
        this(rootDirectory, databaseFileName, configuration, ignored -> {});
    }

    SqliteConnectionFactory(
            Path rootDirectory,
            String databaseFileName,
            SqliteConfiguration configuration,
            BeforeTenantDirectoryCreate beforeTenantDirectoryCreate) {
        if (rootDirectory == null) {
            throw new IllegalArgumentException("rootDirectory must not be null");
        }
        if (databaseFileName == null
                || databaseFileName.isBlank()
                || databaseFileName.contains("/")
                || databaseFileName.contains("\\")
                || databaseFileName.equals(".")
                || databaseFileName.equals("..")) {
            throw new IllegalArgumentException("databaseFileName must be a simple file name");
        }
        if (configuration == null) {
            throw new IllegalArgumentException("configuration must not be null");
        }
        long busyTimeoutMillis = configuration.busyTimeout().toMillis();
        if (busyTimeoutMillis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("busyTimeout must not exceed Integer.MAX_VALUE milliseconds");
        }
        this.rootDirectory = rootDirectory.toAbsolutePath().normalize();
        this.databaseFileName = databaseFileName;
        this.configuration = configuration;
        this.beforeTenantDirectoryCreate = beforeTenantDirectoryCreate;
    }

    public static SqliteConfiguration defaults() {
        return new SqliteConfiguration(
                SqliteJournalMode.WAL, SqliteSynchronousMode.NORMAL, -4_000, Duration.ofSeconds(5), false);
    }

    public Path databasePath(String tenantId) {
        validateIdentifier(tenantId);
        Path path = rootDirectory.resolve(tenantId).resolve(databaseFileName).normalize();
        if (!path.startsWith(rootDirectory)) {
            throw new IllegalArgumentException("Tenant database path escapes its configured root");
        }
        return path;
    }

    public Connection open(String tenantId) throws SQLException {
        validateIdentifier(tenantId);
        Path databasePath = resolveTenantDirectory(tenantId).resolve(databaseFileName);

        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
        try {
            applyPragmas(connection);
            return connection;
        } catch (SQLException exception) {
            try {
                connection.close();
            } catch (SQLException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }

    private Path resolveTenantDirectory(String tenantId) throws SQLException {
        try {
            Path realRoot = createAndValidateRootDirectory();
            Path tenantDirectory = realRoot.resolve(tenantId);
            if (Files.exists(tenantDirectory, LinkOption.NOFOLLOW_LINKS)) {
                validateNormalDirectory(tenantDirectory, "Tenant database directory");
            } else {
                beforeTenantDirectoryCreate.run(tenantDirectory);
                try {
                    Files.createDirectory(tenantDirectory);
                } catch (FileAlreadyExistsException ignored) {
                    validateNormalDirectory(tenantDirectory, "Tenant database directory");
                }
            }
            if (!tenantDirectory.startsWith(realRoot)) {
                throw new SQLException("Tenant database directory escapes its configured root");
            }
            return tenantDirectory;
        } catch (IOException exception) {
            throw new SQLException("Unable to create or validate tenant database directory", exception);
        }
    }

    private Path createAndValidateRootDirectory() throws IOException, SQLException {
        Path current = rootDirectory.getRoot();
        if (current == null) {
            throw new SQLException("Configured SQLite root must be absolute");
        }
        for (Path component : rootDirectory) {
            current = current.resolve(component);
            if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                validateNormalDirectory(current, "Configured SQLite root");
            } else {
                try {
                    Files.createDirectory(current);
                } catch (FileAlreadyExistsException ignored) {
                    validateNormalDirectory(current, "Configured SQLite root");
                }
            }
        }
        return current;
    }

    private static void validateNormalDirectory(Path directory, String description) throws IOException, SQLException {
        BasicFileAttributes attributes =
                Files.readAttributes(directory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isDirectory() || attributes.isSymbolicLink() || attributes.isOther()) {
            throw new SQLException(description + " must be a normal directory, not a symbolic link or reparse point");
        }
    }

    private void applyPragmas(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    "PRAGMA journal_mode=" + configuration.journalMode().name());
            statement.execute(
                    "PRAGMA synchronous=" + configuration.synchronousMode().name());
            statement.execute("PRAGMA cache_size=" + configuration.cacheSizeKb());
            statement.execute(
                    "PRAGMA busy_timeout=" + configuration.busyTimeout().toMillis());
            statement.execute("PRAGMA foreign_keys=ON");
            statement.execute("PRAGMA temp_store=MEMORY");
        }
    }

    private static void validateIdentifier(String value) {
        if (value == null
                || !IDENTIFIER.matcher(value).matches()
                || value.equals(".")
                || value.equals("..")
                || WINDOWS_RESERVED.contains(value.toUpperCase(Locale.ROOT))) {
            throw new IllegalArgumentException("tenantId is not a valid identifier");
        }
    }

    @FunctionalInterface
    interface BeforeTenantDirectoryCreate {

        void run(Path tenantDirectory) throws SQLException;
    }
}
