package com.artemsydorovych.playback.api.model;

import java.time.Instant;

/**
 * Item in "Continue Watching" list - Netflix-style row.
 */
public record ContinueWatchingItem(
    String userId,
    String contentId,
    String title,
    long lastPosition,
    long duration,
    double completionPercentage,
    Instant lastWatched
) {
    public String formattedPosition() {
        long seconds = lastPosition / 1000;
        return String.format("%d:%02d", seconds / 60, seconds % 60);
    }

    public String formattedRemaining() {
        long remaining = Math.max(0, duration - lastPosition) / 1000;
        if (remaining >= 3600) {
            return String.format("%dh %dm left", remaining / 3600, (remaining % 3600) / 60);
        }
        return String.format("%dm left", remaining / 60);
    }
}
