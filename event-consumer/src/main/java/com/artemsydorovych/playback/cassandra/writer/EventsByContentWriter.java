package com.artemsydorovych.playback.cassandra.writer;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.config.CassandraConfig;

import java.time.Instant;

/**
 * Writes events to events_by_content table.
 * Query pattern: "Who is watching this content?" (content analytics)
 */
public class EventsByContentWriter extends AbstractTableWriter {

    private static final long serialVersionUID = 1L;

    @Override
    public String getTableName() {
        return CassandraConfig.TABLE_EVENTS_BY_CONTENT;
    }

    @Override
    protected String getCql() {
        return "INSERT INTO events_by_content " +
               "(content_id, timestamp, user_id, event_id, event_type, session_id, position, device_type, country) " +
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    public boolean accepts(PlaybackEvent event) {
        return true; // All events tracked for content analytics
    }

    @Override
    public void write(PlaybackEvent event) {
        ensureOpen();
        Instant timestamp = toInstant(event);

        session.execute(preparedStatement.bind(
            event.getContentId(),
            timestamp,
            event.getUserId(),
            event.getEventId(),
            event.getEventType().name(),
            event.getSessionId(),
            event.getPosition(),
            extractDeviceType(event),
            extractCountry(event)
        ));
    }
}
