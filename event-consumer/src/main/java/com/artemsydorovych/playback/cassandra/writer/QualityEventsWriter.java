package com.artemsydorovych.playback.cassandra.writer;

import com.artemsydorovych.playback.avro.EventType;
import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.avro.QualityChangePayload;
import com.artemsydorovych.playback.avro.RebufferPayload;
import com.artemsydorovych.playback.config.CassandraConfig;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Set;

/**
 * Writes to playback_quality_events table.
 * Query pattern: Track rebuffering and quality changes for QoS monitoring.
 */
public class QualityEventsWriter extends AbstractTableWriter {

    private static final long serialVersionUID = 1L;

    private static final Set<EventType> QUALITY_EVENTS = Set.of(
        EventType.QUALITY_CHANGE,
        EventType.REBUFFER_START,
        EventType.REBUFFER_END
    );

    @Override
    public String getTableName() {
        return CassandraConfig.TABLE_QUALITY_EVENTS;
    }

    @Override
    protected String getCql() {
        return "INSERT INTO playback_quality_events " +
               "(content_id, event_date, timestamp, event_id, event_type, user_id, session_id, " +
               "device_type, country, old_quality, new_quality, bitrate, rebuffer_duration) " +
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    public boolean accepts(PlaybackEvent event) {
        return QUALITY_EVENTS.contains(event.getEventType());
    }

    @Override
    public void write(PlaybackEvent event) {
        ensureOpen();

        Instant timestamp = toInstant(event);
        LocalDate eventDate = timestamp.atOffset(ZoneOffset.UTC).toLocalDate();

        // Extract quality-specific data
        String oldQuality = null;
        String newQuality = null;
        Integer bitrate = null;
        Long rebufferDuration = null;

        Object payload = event.getPayload();
        if (payload instanceof QualityChangePayload qcp) {
            oldQuality = qcp.getOldQuality().name();
            newQuality = qcp.getNewQuality().name();
            bitrate = qcp.getNewBitrate();
        } else if (payload instanceof RebufferPayload rp) {
            bitrate = rp.getBandwidthEstimate();
            rebufferDuration = rp.getRebufferDuration();
        }

        session.execute(preparedStatement.bind(
            event.getContentId(),
            eventDate,
            timestamp,
            event.getEventId(),
            event.getEventType().name(),
            event.getUserId(),
            event.getSessionId(),
            extractDeviceType(event),
            extractCountry(event),
            oldQuality,
            newQuality,
            bitrate,
            rebufferDuration
        ));
    }
}
