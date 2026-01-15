package com.artemsydorovych.playback.api.model;

import java.time.Instant;

/**
 * DTO for playback events returned by API.
 */
public record PlaybackEventDto(
    String eventId,
    String eventType,
    Instant timestamp,
    String userId,
    String sessionId,
    String contentId,
    String title,
    Long position,
    Long duration,
    String deviceType,
    String country,
    String payloadJson
) {}
