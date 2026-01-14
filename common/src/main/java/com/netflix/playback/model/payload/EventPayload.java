package com.netflix.playback.model.payload;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Container for event-specific payload data.
 * Only one field will be populated based on the event type.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EventPayload {

    /** Payload for SEEK events */
    private SeekPayload seek;

    /** Payload for QUALITY_CHANGE events */
    private QualityChangePayload qualityChange;

    /** Payload for REBUFFER_START and REBUFFER_END events */
    private RebufferPayload rebuffer;

    /** Payload for PLAYBACK_END events */
    private PlaybackEndPayload playbackEnd;

    /** Payload for PROGRESS events */
    private ProgressPayload progress;

    public static EventPayload ofSeek(SeekPayload payload) {
        return EventPayload.builder().seek(payload).build();
    }

    public static EventPayload ofQualityChange(QualityChangePayload payload) {
        return EventPayload.builder().qualityChange(payload).build();
    }

    public static EventPayload ofRebuffer(RebufferPayload payload) {
        return EventPayload.builder().rebuffer(payload).build();
    }

    public static EventPayload ofPlaybackEnd(PlaybackEndPayload payload) {
        return EventPayload.builder().playbackEnd(payload).build();
    }

    public static EventPayload ofProgress(ProgressPayload payload) {
        return EventPayload.builder().progress(payload).build();
    }
}
