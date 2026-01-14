package com.netflix.playback.model.payload;

import com.netflix.playback.model.VideoQuality;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Payload for QUALITY_CHANGE events.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QualityChangePayload {

    /** Quality before change */
    private VideoQuality oldQuality;

    /** Quality after change */
    private VideoQuality newQuality;

    /** Bitrate before change (in kbps) */
    private int oldBitrate;

    /** Bitrate after change (in kbps) */
    private int newBitrate;

    /** Reason for quality change */
    private String reason;

    /** Whether this was an automatic adaptation or user-initiated */
    private boolean automatic;
}
