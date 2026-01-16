package com.artemsydorovych.playback.api.service;

import com.artemsydorovych.playback.api.model.ContinueWatchingItem;
import com.artemsydorovych.playback.api.model.PlaybackEventDto;
import com.artemsydorovych.playback.api.model.UserProgress;
import com.artemsydorovych.playback.cassandra.CassandraClient;
import com.artemsydorovych.playback.config.CassandraConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Service for reading playback data from Cassandra.
 * Stateless - each method opens and closes its own queries.
 */
public class PlaybackService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PlaybackService.class);

    private final CassandraClient client;
    private final CqlSession session;

    // Prepared statements
    private final PreparedStatement selectContinueWatching;
    private final PreparedStatement selectUserProgress;
    private final PreparedStatement selectUserProgressByContent;
    private final PreparedStatement selectEventsByUser;
    private final PreparedStatement selectEventsBySession;
    private final PreparedStatement selectEventsByContent;

    public PlaybackService() {
        this(new CassandraClient());
    }

    public PlaybackService(CassandraClient client) {
        this.client = client;
        this.session = client.getSession();

        // Prepare all read statements
        this.selectContinueWatching = session.prepare(
            "SELECT user_id, content_id, title, last_position, duration, completion_percentage, last_watched " +
            "FROM continue_watching WHERE user_id = ? LIMIT ?"
        );

        this.selectUserProgress = session.prepare(
            "SELECT user_id, content_id, title, last_position, duration, completion_percentage, " +
            "last_watched, total_watch_time, session_id, device_type, country " +
            "FROM user_content_progress WHERE user_id = ?"
        );

        this.selectUserProgressByContent = session.prepare(
            "SELECT user_id, content_id, title, last_position, duration, completion_percentage, " +
            "last_watched, total_watch_time, session_id, device_type, country " +
            "FROM user_content_progress WHERE user_id = ? AND content_id = ?"
        );

        this.selectEventsByUser = session.prepare(
            "SELECT event_id, event_type, timestamp, user_id, session_id, content_id, title, " +
            "position, duration, device_type " +
            "FROM events_by_user WHERE user_id = ? LIMIT ?"
        );

        this.selectEventsBySession = session.prepare(
            "SELECT event_id, event_type, timestamp, user_id, session_id, content_id, title, " +
            "position, duration, payload_json " +
            "FROM events_by_session WHERE session_id = ?"
        );

        this.selectEventsByContent = session.prepare(
            "SELECT event_id, event_type, timestamp, user_id, session_id, content_id, " +
            "position, device_type, country " +
            "FROM events_by_content WHERE content_id = ? LIMIT ?"
        );

        log.info("PlaybackService initialized");
    }

    /**
     * Get "Continue Watching" list for a user (streaming platform).
     */
    public List<ContinueWatchingItem> getContinueWatching(String userId, int limit) {
        ResultSet rs = session.execute(selectContinueWatching.bind(userId, limit));
        List<ContinueWatchingItem> items = new ArrayList<>();

        for (Row row : rs) {
            items.add(new ContinueWatchingItem(
                row.getString("user_id"),
                row.getString("content_id"),
                row.getString("title"),
                row.getLong("last_position"),
                row.getLong("duration"),
                row.getDouble("completion_percentage"),
                row.getInstant("last_watched")
            ));
        }

        return items;
    }

    /**
     * Get user's progress on a specific content (for "Resume Playback").
     */
    public Optional<UserProgress> getProgress(String userId, String contentId) {
        ResultSet rs = session.execute(selectUserProgressByContent.bind(userId, contentId));
        Row row = rs.one();

        if (row == null) {
            return Optional.empty();
        }

        return Optional.of(mapToUserProgress(row));
    }

    /**
     * Get all content progress for a user.
     */
    public List<UserProgress> getAllProgress(String userId) {
        ResultSet rs = session.execute(selectUserProgress.bind(userId));
        List<UserProgress> items = new ArrayList<>();

        for (Row row : rs) {
            items.add(mapToUserProgress(row));
        }

        return items;
    }

    /**
     * Get user's event history.
     */
    public List<PlaybackEventDto> getEventsByUser(String userId, int limit) {
        ResultSet rs = session.execute(selectEventsByUser.bind(userId, limit));
        List<PlaybackEventDto> events = new ArrayList<>();

        for (Row row : rs) {
            events.add(new PlaybackEventDto(
                row.getString("event_id"),
                row.getString("event_type"),
                row.getInstant("timestamp"),
                row.getString("user_id"),
                row.getString("session_id"),
                row.getString("content_id"),
                row.getString("title"),
                row.get("position", Long.class),
                row.get("duration", Long.class),
                row.getString("device_type"),
                null,
                null
            ));
        }

        return events;
    }

    /**
     * Get all events for a session (session replay).
     */
    public List<PlaybackEventDto> getEventsBySession(String sessionId) {
        ResultSet rs = session.execute(selectEventsBySession.bind(sessionId));
        List<PlaybackEventDto> events = new ArrayList<>();

        for (Row row : rs) {
            events.add(new PlaybackEventDto(
                row.getString("event_id"),
                row.getString("event_type"),
                row.getInstant("timestamp"),
                row.getString("user_id"),
                row.getString("session_id"),
                row.getString("content_id"),
                row.getString("title"),
                row.get("position", Long.class),
                row.get("duration", Long.class),
                null,
                null,
                row.getString("payload_json")
            ));
        }

        return events;
    }

    /**
     * Get events for specific content (content analytics).
     */
    public List<PlaybackEventDto> getEventsByContent(String contentId, int limit) {
        ResultSet rs = session.execute(selectEventsByContent.bind(contentId, limit));
        List<PlaybackEventDto> events = new ArrayList<>();

        for (Row row : rs) {
            events.add(new PlaybackEventDto(
                row.getString("event_id"),
                row.getString("event_type"),
                row.getInstant("timestamp"),
                row.getString("user_id"),
                row.getString("session_id"),
                row.getString("content_id"),
                null,
                row.get("position", Long.class),
                null,
                row.getString("device_type"),
                row.getString("country"),
                null
            ));
        }

        return events;
    }

    private UserProgress mapToUserProgress(Row row) {
        return new UserProgress(
            row.getString("user_id"),
            row.getString("content_id"),
            row.getString("title"),
            row.getLong("last_position"),
            row.getLong("duration"),
            row.getDouble("completion_percentage"),
            row.getInstant("last_watched"),
            row.getLong("total_watch_time"),
            row.getString("session_id"),
            row.getString("device_type"),
            row.getString("country")
        );
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}
