package com.netflix.playback.model.payload;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Payload for REBUFFER_START and REBUFFER_END events.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RebufferPayload {

    /** Buffer length in milliseconds when rebuffering started */
    private long bufferLength;

    /** Duration of rebuffering in milliseconds (only set for REBUFFER_END) */
    private Long rebufferDuration;

    /** Current bandwidth estimate (in kbps) */
    private int bandwidthEstimate;
}
