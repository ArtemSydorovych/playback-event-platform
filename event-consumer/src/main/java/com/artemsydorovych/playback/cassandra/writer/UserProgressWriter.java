package com.artemsydorovych.playback.cassandra.writer;

import com.artemsydorovych.playback.avro.EventType;
import com.artemsydorovych.playback.avro.PlaybackEndPayload;
import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.config.CassandraConfig;

import java.time.Instant;
import java.util.Set;

/**
 * Writes to user_content_progress table.
 * Query pattern: "Where did user leave off on this content?" (resume playback)
 */
public class UserProgressWriter extends AbstractTableWriter {

    private static final long serialVersionUID = 1L;

    private static final Set<EventType> PROGRESS_EVENTS = Set.of(
        EventType.PLAY_START,
        EventType.PROGRESS,
        EventType.PAUSE,
        EventType.SEEK,
        EventType.PLAYBACK_END
    );

    @Override
    public String getTableName() {
        return CassandraConfig.TABLE_USER_CONTENT_PROGRESS;
    }

    @Override
    protected String getCql() {
        return "INSERT INTO user_content_progress " +
               "(user_id, content_id, title, last_position, duration, completion_percentage, " +
               "last_watched, total_watch_time, session_id, device_type, country) " +
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    public boolean accepts(PlaybackEvent event) {
        return PROGRESS_EVENTS.contains(event.getEventType()) &&
               event.getPosition() != null &&
               event.getDuration() != null &&
               event.getDuration() > 0;
    }

    @Override
    public void write(PlaybackEvent event) {
        ensureOpen();

        Instant timestamp = toInstant(event);
        long position = event.getPosition();
        long duration = event.getDuration();
        double completionPercentage = (position * 100.0) / duration;
        long totalWatchTime = 0;

        // Extract additional data from PLAYBACK_END
        if (event.getEventType() == EventType.PLAYBACK_END &&
            event.getPayload() instanceof PlaybackEndPayload) {
            PlaybackEndPayload payload = (PlaybackEndPayload) event.getPayload();
            totalWatchTime = payload.getTotalWatchTime();
            completionPercentage = payload.getCompletionPercentage();
        }

        session.execute(preparedStatement.bind(
            event.getUserId(),
            event.getContentId(),
            event.getTitle(),
            position,
            duration,
            completionPercentage,
            timestamp,
            totalWatchTime,
            event.getSessionId(),
            extractDeviceType(event),
            extractCountry(event)
        ));
    }
}
