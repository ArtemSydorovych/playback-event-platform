package com.artemsydorovych.playback.cassandra.writer;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.config.CassandraConfig;

import java.time.Instant;

/**
 * Writes events to events_by_user table.
 * Query pattern: "What has this user watched?" (user activity timeline)
 */
public class EventsByUserWriter extends AbstractTableWriter {

    private static final long serialVersionUID = 1L;

    @Override
    public String getTableName() {
        return CassandraConfig.TABLE_EVENTS_BY_USER;
    }

    @Override
    protected String getCql() {
        return "INSERT INTO events_by_user " +
               "(user_id, timestamp, event_id, event_type, session_id, content_id, title, position, duration, device_type) " +
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    public boolean accepts(PlaybackEvent event) {
        return true; // All events go to user timeline
    }

    @Override
    public void write(PlaybackEvent event) {
        ensureOpen();
        Instant timestamp = toInstant(event);

        session.execute(preparedStatement.bind(
            event.getUserId(),
            timestamp,
            event.getEventId(),
            event.getEventType().name(),
            event.getSessionId(),
            event.getContentId(),
            event.getTitle(),
            event.getPosition(),
            event.getDuration(),
            extractDeviceType(event)
        ));
    }
}
