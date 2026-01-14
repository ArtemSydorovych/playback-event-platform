package com.netflix.playback.model.payload;

import com.netflix.playback.model.PlaybackEndReason;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Payload for PLAYBACK_END events.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlaybackEndPayload {

    /** Reason why playback ended */
    private PlaybackEndReason reason;

    /** Completion percentage (0.0 - 100.0) */
    private double completionPercentage;

    /** Total watch time in this session (milliseconds) */
    private long watchDuration;

    /** Error code if reason is ERROR */
    private String errorCode;

    /** Error message if reason is ERROR */
    private String errorMessage;
}
