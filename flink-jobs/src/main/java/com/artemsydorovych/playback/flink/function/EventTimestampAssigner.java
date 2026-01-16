package com.artemsydorovych.playback.flink.function;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.flink.config.FlinkConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * Timestamp assigner and watermark strategy for PlaybackEvents.
 * Uses event time from the event's timestamp field.
 */
public class EventTimestampAssigner {

    private EventTimestampAssigner() {}

    /**
     * Creates a WatermarkStrategy for PlaybackEvents.
     * Uses bounded out-of-orderness to handle late events.
     */
    public static WatermarkStrategy<PlaybackEvent> create() {
        return create(FlinkConfig.MAX_OUT_OF_ORDERNESS, FlinkConfig.IDLE_TIMEOUT);
    }

    /**
     * Creates a WatermarkStrategy with custom parameters.
     *
     * @param maxOutOfOrderness Maximum time an event can be late
     * @param idleTimeout       Time after which a source is considered idle
     */
    public static WatermarkStrategy<PlaybackEvent> create(
            Duration maxOutOfOrderness,
            Duration idleTimeout) {

        return WatermarkStrategy
            .<PlaybackEvent>forBoundedOutOfOrderness(maxOutOfOrderness)
            .withIdleness(idleTimeout)
            .withTimestampAssigner(new PlaybackEventTimestampAssigner());
    }

    /**
     * Extracts the event timestamp from PlaybackEvent.
     */
    private static class PlaybackEventTimestampAssigner
            implements SerializableTimestampAssigner<PlaybackEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(PlaybackEvent event, long recordTimestamp) {
            // The Avro timestamp-millis logical type returns Instant directly
            if (event.getTimestamp() != null) {
                return event.getTimestamp().toEpochMilli();
            }
            // Fallback to record timestamp (from Kafka) if event timestamp is missing
            return recordTimestamp;
        }
    }
}
