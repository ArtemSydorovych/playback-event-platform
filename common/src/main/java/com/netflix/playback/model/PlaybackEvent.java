package com.netflix.playback.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.netflix.playback.model.payload.EventPayload;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Core playback event model.
 * Represents any interaction a user has with video playback.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PlaybackEvent {

    /** Unique identifier for this event */
    @Builder.Default
    private String eventId = UUID.randomUUID().toString();

    /** Type of playback event */
    private EventType eventType;

    /** Timestamp when event occurred on the client */
    @Builder.Default
    private Instant timestamp = Instant.now();

    /** User account identifier */
    private String userId;

    /** Profile within the user account (for multi-profile accounts) */
    private String profileId;

    /** Unique device identifier */
    private String deviceId;

    /** Session identifier (unique per playback session) */
    private String sessionId;

    /** Content being watched */
    private String contentId;

    /** Current playback position in milliseconds */
    private Long position;

    /** Total content duration in milliseconds */
    private Long contentDuration;

    /** Device information */
    private DeviceInfo deviceInfo;

    /** Event-specific payload data */
    private EventPayload payload;

    /**
     * Calculate completion percentage based on position and duration.
     */
    public double getCompletionPercentage() {
        if (contentDuration == null || contentDuration == 0 || position == null) {
            return 0.0;
        }
        return (position * 100.0) / contentDuration;
    }

    /**
     * Check if this event indicates content completion (>= 90%).
     */
    public boolean isContentCompleted() {
        return getCompletionPercentage() >= 90.0;
    }

    /**
     * Get formatted position as HH:MM:SS.
     */
    public String getFormattedPosition() {
        if (position == null) return "00:00:00";
        long totalSeconds = position / 1000;
        long hours = totalSeconds / 3600;
        long minutes = (totalSeconds % 3600) / 60;
        long seconds = totalSeconds % 60;
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    @Override
    public String toString() {
        return String.format(
            "PlaybackEvent{type=%s, user=%s, content=%s, position=%s, device=%s}",
            eventType,
            userId,
            contentId,
            getFormattedPosition(),
            deviceInfo != null ? deviceInfo.getDeviceType() : "unknown"
        );
    }
}
