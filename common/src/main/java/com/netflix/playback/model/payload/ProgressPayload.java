package com.netflix.playback.model.payload;

import com.netflix.playback.model.VideoQuality;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Payload for PROGRESS (heartbeat) events.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProgressPayload {

    /** Current buffer length in milliseconds */
    private long bufferLength;

    /** Current video quality */
    private VideoQuality currentQuality;

    /** Current bitrate in kbps */
    private int currentBitrate;

    /** Frames dropped since last progress event */
    private int droppedFrames;

    /** Current playback rate (1.0 = normal speed) */
    private double playbackRate;

    /** Audio language currently playing */
    private String audioLanguage;

    /** Whether subtitles are enabled */
    private boolean subtitlesEnabled;
}
