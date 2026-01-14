package com.netflix.playback.generator;

import com.netflix.playback.model.*;
import com.netflix.playback.model.payload.*;

import java.util.Random;
import java.util.UUID;

/**
 * Generates simulated playback events.
 */
public class EventSimulator {

    private static final Random RANDOM = new Random();

    private static final String[] CONTENT_IDS = {
        "movie-001", "movie-002", "movie-003",
        "series-001-e01", "series-001-e02", "series-002-e01"
    };

    private static final long[] DURATIONS = {
        7200000, 5400000, 6300000,  // movies ~2hrs
        2700000, 2400000, 3600000   // episodes ~45min
    };

    private static final String[] COUNTRIES = {"US", "UK", "DE", "FR", "JP"};

    public static PlaybackEvent generateRandomEvent() {
        String userId = "user-" + RANDOM.nextInt(100);
        String contentId = CONTENT_IDS[RANDOM.nextInt(CONTENT_IDS.length)];
        long duration = DURATIONS[RANDOM.nextInt(DURATIONS.length)];
        long position = RANDOM.nextLong(duration);
        EventType eventType = randomEventType();

        PlaybackEvent.PlaybackEventBuilder builder = PlaybackEvent.builder()
            .eventType(eventType)
            .userId(userId)
            .profileId("profile-1")
            .deviceId("device-" + RANDOM.nextInt(50))
            .sessionId("session-" + UUID.randomUUID().toString().substring(0, 8))
            .contentId(contentId)
            .position(position)
            .contentDuration(duration)
            .deviceInfo(randomDeviceInfo());

        // Add payload based on event type
        addPayload(builder, eventType, position, duration);

        return builder.build();
    }

    private static void addPayload(PlaybackEvent.PlaybackEventBuilder builder,
                                   EventType type, long position, long duration) {
        switch (type) {
            case SEEK -> {
                long toPosition = RANDOM.nextLong(duration);
                builder.payload(EventPayload.ofSeek(SeekPayload.builder()
                    .fromPosition(position)
                    .toPosition(toPosition)
                    .build()));
            }
            case PROGRESS -> builder.payload(EventPayload.ofProgress(ProgressPayload.builder()
                .bufferLength(30000 + RANDOM.nextInt(30000))
                .currentQuality(VideoQuality.HD_1080P)
                .currentBitrate(5000)
                .droppedFrames(RANDOM.nextInt(3))
                .playbackRate(1.0)
                .build()));
            case PLAYBACK_END -> builder.payload(EventPayload.ofPlaybackEnd(PlaybackEndPayload.builder()
                .reason(PlaybackEndReason.USER_STOPPED)
                .completionPercentage((position * 100.0) / duration)
                .watchDuration(position)
                .build()));
            case QUALITY_CHANGE -> builder.payload(EventPayload.ofQualityChange(QualityChangePayload.builder()
                .oldQuality(VideoQuality.HD_1080P)
                .newQuality(VideoQuality.HD_720P)
                .oldBitrate(5000)
                .newBitrate(3000)
                .reason("bandwidth")
                .automatic(true)
                .build()));
            default -> {} // No payload needed
        }
    }

    private static EventType randomEventType() {
        EventType[] types = {
            EventType.PLAY_START, EventType.PROGRESS, EventType.PROGRESS,
            EventType.PAUSE, EventType.RESUME, EventType.SEEK,
            EventType.PLAYBACK_END
        };
        return types[RANDOM.nextInt(types.length)];
    }

    private static DeviceInfo randomDeviceInfo() {
        DeviceType[] types = DeviceType.values();
        DeviceType type = types[RANDOM.nextInt(types.length)];

        return DeviceInfo.builder()
            .deviceType(type)
            .deviceModel(type.name() + "-Model")
            .osVersion("1.0")
            .appVersion("8.0.0")
            .country(COUNTRIES[RANDOM.nextInt(COUNTRIES.length)])
            .build();
    }
}
