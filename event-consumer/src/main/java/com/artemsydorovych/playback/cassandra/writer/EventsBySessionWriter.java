package com.artemsydorovych.playback.cassandra.writer;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.config.CassandraConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;

/**
 * Writes events to events_by_session table.
 * Query pattern: "What happened in this playback session?" (session replay/debugging)
 */
public class EventsBySessionWriter extends AbstractTableWriter {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public String getTableName() {
        return CassandraConfig.TABLE_EVENTS_BY_SESSION;
    }

    @Override
    protected String getCql() {
        return "INSERT INTO events_by_session " +
               "(session_id, timestamp, event_id, event_type, user_id, content_id, title, position, duration, payload_json) " +
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    public void open(com.datastax.oss.driver.api.core.CqlSession session) {
        super.open(session);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public boolean accepts(PlaybackEvent event) {
        return true; // All events go to session log
    }

    @Override
    public void write(PlaybackEvent event) {
        ensureOpen();
        Instant timestamp = toInstant(event);
        String payloadJson = serializePayload(event.getPayload());

        session.execute(preparedStatement.bind(
            event.getSessionId(),
            timestamp,
            event.getEventId(),
            event.getEventType().name(),
            event.getUserId(),
            event.getContentId(),
            event.getTitle(),
            event.getPosition(),
            event.getDuration(),
            payloadJson
        ));
    }

    private String serializePayload(Object payload) {
        if (payload == null || objectMapper == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize payload: {}", e.getMessage());
            return null;
        }
    }
}
