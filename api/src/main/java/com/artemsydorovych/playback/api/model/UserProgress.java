package com.artemsydorovych.playback.api.model;

import java.time.Instant;

/**
 * User's progress on specific content - for "Resume Playback" feature.
 */
public record UserProgress(
    String userId,
    String contentId,
    String title,
    long lastPosition,
    long duration,
    double completionPercentage,
    Instant lastWatched,
    long totalWatchTime,
    String sessionId,
    String deviceType,
    String country
) {
    public String formattedPosition() {
        long seconds = lastPosition / 1000;
        return String.format("%d:%02d", seconds / 60, seconds % 60);
    }

    public String formattedDuration() {
        long seconds = duration / 1000;
        return String.format("%d:%02d", seconds / 60, seconds % 60);
    }

    public long remainingTime() {
        return Math.max(0, duration - lastPosition);
    }
}
