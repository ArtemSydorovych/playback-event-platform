package com.artemsydorovych.playback.cassandra.writer;

import com.artemsydorovych.playback.avro.EventType;
import com.artemsydorovych.playback.avro.PlaybackEndPayload;
import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.config.CassandraConfig;

import java.time.Instant;
import java.util.Set;

/**
 * Writes to continue_watching table.
 * Query pattern: "User's incomplete content sorted by recency" (Continue Watching row)
 *
 * Only stores content that is:
 * - Started (>2% progress) but not completed (<95%)
 * - Sorted by last_watched timestamp
 */
public class ContinueWatchingWriter extends AbstractTableWriter {

    private static final long serialVersionUID = 1L;

    private static final Set<EventType> TRACKABLE_EVENTS = Set.of(
        EventType.PROGRESS,
        EventType.PAUSE,
        EventType.PLAYBACK_END
    );

    @Override
    public String getTableName() {
        return CassandraConfig.TABLE_CONTINUE_WATCHING;
    }

    @Override
    protected String getCql() {
        return "INSERT INTO continue_watching " +
               "(user_id, last_watched, content_id, title, last_position, duration, completion_percentage) " +
               "VALUES (?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    public boolean accepts(PlaybackEvent event) {
        if (!TRACKABLE_EVENTS.contains(event.getEventType())) {
            return false;
        }

        Long position = event.getPosition();
        Long duration = event.getDuration();

        if (position == null || duration == null || duration == 0) {
            return false;
        }

        double completionPercentage = calculateCompletion(event, position, duration);

        // Only track if started but not completed
        return completionPercentage >= CassandraConfig.MIN_PROGRESS_THRESHOLD_PERCENT &&
               completionPercentage < CassandraConfig.COMPLETION_THRESHOLD_PERCENT;
    }

    @Override
    public void write(PlaybackEvent event) {
        ensureOpen();

        Instant timestamp = toInstant(event);
        long position = event.getPosition();
        long duration = event.getDuration();
        double completionPercentage = calculateCompletion(event, position, duration);

        session.execute(preparedStatement.bind(
            event.getUserId(),
            timestamp,
            event.getContentId(),
            event.getTitle(),
            position,
            duration,
            completionPercentage
        ));
    }

    private double calculateCompletion(PlaybackEvent event, long position, long duration) {
        // Use payload completion if available (more accurate)
        if (event.getEventType() == EventType.PLAYBACK_END &&
            event.getPayload() instanceof PlaybackEndPayload) {
            return ((PlaybackEndPayload) event.getPayload()).getCompletionPercentage();
        }
        return (position * 100.0) / duration;
    }
}
