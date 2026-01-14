package com.netflix.playback.model;

/**
 * Reasons why playback ended.
 */
public enum PlaybackEndReason {
    /** Content finished playing (reached end) */
    COMPLETED,

    /** User explicitly stopped playback */
    USER_STOPPED,

    /** User navigated away or closed app */
    USER_EXIT,

    /** Playback failed due to error */
    ERROR,

    /** Network connection lost */
    NETWORK_ERROR,

    /** App crashed or was killed */
    APP_TERMINATED
}
