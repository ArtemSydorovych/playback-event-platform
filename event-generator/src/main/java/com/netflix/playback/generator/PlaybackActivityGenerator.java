package com.netflix.playback.generator;

import com.netflix.playback.avro.*;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates realistic playback activity events for testing and simulation.
 * <p>
 * This generator creates events that mimic real user viewing behavior,
 * including play/pause actions, seeking, quality changes, buffering events,
 * and session completions.
 */
public class PlaybackActivityGenerator {

    private static final String[] CONTENT_CATALOG = {
            "movie-001", "movie-002", "movie-003",
            "series-s01e01", "series-s01e02", "series-s02e01",
            "doc-001", "doc-002"
    };

    private static final String[] ACTIVE_USERS = {
            "user-1001", "user-1002", "user-1003", "user-1004", "user-1005",
            "user-2001", "user-2002", "user-2003"
    };

    private static final String[] CONTENT_TITLES = {
            "The Great Adventure", "Mystery of the Deep", "Comedy Hour",
            "Drama Series S1E1", "Drama Series S1E2", "Drama Series S2E1",
            "Nature Documentary", "Space Exploration"
    };

    private static final String[] DEVICE_MODELS = {
            "iPhone 15 Pro", "iPhone 14", "Samsung Galaxy S24", "Pixel 8",
            "Samsung Smart TV", "LG OLED", "Apple TV 4K", "Roku Ultra",
            "Chrome Browser", "Safari Browser", "Firefox Browser"
    };

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    /**
     * Generate a random playback event simulating user viewing activity.
     */
    public PlaybackEvent generate() {
        String userId = ACTIVE_USERS[random.nextInt(ACTIVE_USERS.length)];
        int contentIndex = random.nextInt(CONTENT_CATALOG.length);
        String contentId = CONTENT_CATALOG[contentIndex];
        String title = CONTENT_TITLES[contentIndex];
        EventType eventType = selectEventType();

        long duration = 3600000L + random.nextInt(3600000); // 1-2 hours
        long position = random.nextLong(duration);

        PlaybackEvent.Builder builder = PlaybackEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(eventType)
                .setTimestamp(Instant.now())
                .setUserId(userId)
                .setDeviceId("device-" + userId.replace("user-", ""))
                .setSessionId(UUID.randomUUID().toString())
                .setContentId(contentId)
                .setContentType(determineContentType(contentId))
                .setTitle(title)
                .setPosition(position)
                .setDuration(duration)
                .setDeviceInfo(generateDeviceInfo());

        attachPayload(builder, eventType, position, duration);

        return builder.build();
    }

    /**
     * Generate a specific event type for a given user and content.
     */
    public PlaybackEvent generateForUser(String userId, String contentId, EventType eventType) {
        long duration = 3600000L;
        long position = random.nextLong(duration);

        PlaybackEvent.Builder builder = PlaybackEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(eventType)
                .setTimestamp(Instant.now())
                .setUserId(userId)
                .setDeviceId("device-" + userId.replace("user-", ""))
                .setSessionId(UUID.randomUUID().toString())
                .setContentId(contentId)
                .setPosition(position)
                .setDuration(duration)
                .setDeviceInfo(generateDeviceInfo());

        attachPayload(builder, eventType, position, duration);

        return builder.build();
    }

    private EventType selectEventType() {
        // Weight distribution to simulate realistic user behavior
        int weight = random.nextInt(100);
        if (weight < 30) return EventType.PROGRESS;      // 30% - heartbeats are most common
        if (weight < 45) return EventType.PLAY_START;    // 15%
        if (weight < 55) return EventType.PAUSE;         // 10%
        if (weight < 65) return EventType.RESUME;        // 10%
        if (weight < 75) return EventType.SEEK;          // 10%
        if (weight < 85) return EventType.QUALITY_CHANGE;// 10%
        if (weight < 90) return EventType.REBUFFER_START;// 5%
        if (weight < 95) return EventType.REBUFFER_END;  // 5%
        return EventType.PLAYBACK_END;                   // 5%
    }

    private String determineContentType(String contentId) {
        if (contentId.startsWith("movie")) return "movie";
        if (contentId.startsWith("series")) return "episode";
        return "documentary";
    }

    private DeviceInfo generateDeviceInfo() {
        DeviceType deviceType = DeviceType.values()[random.nextInt(DeviceType.values().length)];
        String deviceModel = DEVICE_MODELS[random.nextInt(DEVICE_MODELS.length)];

        return DeviceInfo.newBuilder()
                .setDeviceType(deviceType)
                .setDeviceId("dev-" + random.nextInt(10000))
                .setDeviceModel(deviceModel)
                .setOsName(getOsForDeviceType(deviceType))
                .setOsVersion(random.nextInt(10) + "." + random.nextInt(10))
                .setAppVersion("2." + random.nextInt(10) + "." + random.nextInt(100))
                .setIpAddress(generateIpAddress())
                .setCountry(random.nextBoolean() ? "US" : "GB")
                .setRegion(random.nextBoolean() ? "CA" : "NY")
                .setCity(random.nextBoolean() ? "Los Angeles" : "New York")
                .build();
    }

    private String getOsForDeviceType(DeviceType deviceType) {
        return switch (deviceType) {
            case IOS -> "iOS";
            case ANDROID -> "Android";
            case WEB -> "Web";
            case SMART_TV -> "Tizen";
            case GAMING_CONSOLE -> "PlayStation OS";
            case SET_TOP_BOX -> "Roku OS";
        };
    }

    private String generateIpAddress() {
        return random.nextInt(256) + "." + random.nextInt(256) + "." +
                random.nextInt(256) + "." + random.nextInt(256);
    }

    private void attachPayload(PlaybackEvent.Builder builder, EventType eventType,
                               long position, long duration) {
        switch (eventType) {
            case SEEK -> builder.setPayload(createSeekPayload(position, duration));
            case QUALITY_CHANGE -> builder.setPayload(createQualityChangePayload());
            case REBUFFER_START, REBUFFER_END -> builder.setPayload(createRebufferPayload(eventType));
            case PLAYBACK_END -> builder.setPayload(createPlaybackEndPayload(position, duration));
            case PROGRESS -> builder.setPayload(createProgressPayload());
            default -> builder.setPayload(null); // PLAY_START, PAUSE, RESUME have no payload
        }
    }

    private SeekPayload createSeekPayload(long currentPosition, long duration) {
        return SeekPayload.newBuilder()
                .setFromPosition(currentPosition)
                .setToPosition(random.nextLong(duration))
                .build();
    }

    private QualityChangePayload createQualityChangePayload() {
        VideoQuality[] qualities = VideoQuality.values();
        VideoQuality oldQuality = qualities[random.nextInt(qualities.length)];
        VideoQuality newQuality = qualities[random.nextInt(qualities.length)];

        return QualityChangePayload.newBuilder()
                .setOldQuality(oldQuality)
                .setNewQuality(newQuality)
                .setOldBitrate(getBitrateForQuality(oldQuality))
                .setNewBitrate(getBitrateForQuality(newQuality))
                .setReason(random.nextBoolean() ? "bandwidth_change" : "user_selection")
                .setAutomatic(random.nextBoolean())
                .build();
    }

    private RebufferPayload createRebufferPayload(EventType eventType) {
        RebufferPayload.Builder builder = RebufferPayload.newBuilder()
                .setBufferLength(random.nextInt(5000))
                .setBandwidthEstimate(random.nextInt(20000) + 1000)
                .setNetworkType(random.nextBoolean() ? "wifi" : "cellular");

        if (eventType == EventType.REBUFFER_END) {
            builder.setRebufferDuration((long) random.nextInt(10000));
        }

        return builder.build();
    }

    private PlaybackEndPayload createPlaybackEndPayload(long position, long duration) {
        PlaybackEndReason[] reasons = PlaybackEndReason.values();
        PlaybackEndReason reason = reasons[random.nextInt(reasons.length)];
        double completion = (position * 100.0) / duration;

        PlaybackEndPayload.Builder builder = PlaybackEndPayload.newBuilder()
                .setReason(reason)
                .setFinalPosition(position)
                .setTotalWatchTime(random.nextLong(duration))
                .setCompletionPercentage(completion);

        if (reason == PlaybackEndReason.ERROR || reason == PlaybackEndReason.NETWORK_ERROR) {
            builder.setErrorCode("ERR_" + random.nextInt(1000));
            builder.setErrorMessage("Playback error occurred");
        }

        return builder.build();
    }

    private ProgressPayload createProgressPayload() {
        VideoQuality[] qualities = VideoQuality.values();

        return ProgressPayload.newBuilder()
                .setBufferLength(random.nextInt(30000) + 5000)
                .setCurrentQuality(qualities[random.nextInt(qualities.length)])
                .setCurrentBitrate(random.nextInt(15000) + 1000)
                .setDroppedFrames(random.nextInt(10))
                .setPlaybackRate(1.0)
                .setAudioLanguage("en")
                .setSubtitlesEnabled(random.nextBoolean())
                .setSubtitleLanguage(random.nextBoolean() ? "en" : null)
                .build();
    }

    private int getBitrateForQuality(VideoQuality quality) {
        return switch (quality) {
            case AUTO -> 5000;
            case SD_480P -> 1500;
            case HD_720P -> 3000;
            case HD_1080P -> 5000;
            case UHD_4K -> 15000;
            case UHD_8K -> 30000;
        };
    }
}
