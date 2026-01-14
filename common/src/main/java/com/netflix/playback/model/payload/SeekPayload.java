package com.netflix.playback.model.payload;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Payload for SEEK events.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SeekPayload {

    /** Position before seek (in milliseconds) */
    private long fromPosition;

    /** Position after seek (in milliseconds) */
    private long toPosition;

    /** Whether this was a forward seek */
    public boolean isForwardSeek() {
        return toPosition > fromPosition;
    }

    /** Distance seeked in milliseconds */
    public long getSeekDistance() {
        return Math.abs(toPosition - fromPosition);
    }
}
