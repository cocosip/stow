package io.github.cocosip.stow.internal.quota;

import io.github.cocosip.stow.exception.ProjectionException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

final class QuotaEventLedger {

    void validate(String eventId, long sequenceNumber) {
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (sequenceNumber <= 0) {
            throw new IllegalArgumentException("sequenceNumber must be greater than zero");
        }
    }

    boolean mark(Connection connection, String eventId, long sequenceNumber, long appliedAtMillis) throws SQLException {
        validate(eventId, sequenceNumber);
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT sequence_number FROM applied_quota_events WHERE event_id=?")) {
            statement.setString(1, eventId);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    if (result.getLong(1) != sequenceNumber) {
                        throw new ProjectionException("Quota event ID was already applied with a different sequence");
                    }
                    return false;
                }
            }
        }
        try (PreparedStatement statement =
                connection.prepareStatement("SELECT event_id FROM applied_quota_events WHERE sequence_number=?")) {
            statement.setLong(1, sequenceNumber);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    throw new ProjectionException("Quota sequence was already applied by a different event");
                }
            }
        }
        try (PreparedStatement statement = connection.prepareStatement(
                """
                INSERT INTO applied_quota_events(event_id, sequence_number, applied_at_ms)
                VALUES(?, ?, ?)
                """)) {
            statement.setString(1, eventId);
            statement.setLong(2, sequenceNumber);
            statement.setLong(3, appliedAtMillis);
            statement.executeUpdate();
        }
        return true;
    }
}
