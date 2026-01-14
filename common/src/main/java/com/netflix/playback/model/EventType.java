package com.netflix.playback.model;

/**
 * Types of playback events that can be emitted by video players.
 */
public enum EventType {
    /** User initiated playback */
    PLAY_START,

    /** User paused the video */
    PAUSE,

    /** User resumed after pause */
    RESUME,

    /** User seeked to a different position */
    SEEK,

    /** Periodic heartbeat with current position (every 10-30s) */
    PROGRESS,

    /** Video quality changed (bitrate adaptation) */
    QUALITY_CHANGE,

    /** Playback stalled due to buffering */
    REBUFFER_START,

    /** Playback resumed after buffering */
    REBUFFER_END,

    /** User stopped or content ended */
    PLAYBACK_END
}
