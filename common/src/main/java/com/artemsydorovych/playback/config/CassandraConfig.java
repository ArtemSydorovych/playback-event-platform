package com.artemsydorovych.playback.config;

/**
 * Centralized configuration constants for Cassandra.
 */
public final class CassandraConfig {

    private CassandraConfig() {
        // Prevent instantiation
    }

    // Connection
    public static final String CONTACT_POINT = "localhost";
    public static final int PORT = 9042;
    public static final String LOCAL_DATACENTER = "dc1";

    // Keyspace
    public static final String KEYSPACE = "playback";

    // Table Names
    public static final String TABLE_EVENTS_BY_USER = "events_by_user";
    public static final String TABLE_EVENTS_BY_SESSION = "events_by_session";
    public static final String TABLE_EVENTS_BY_CONTENT = "events_by_content";
    public static final String TABLE_USER_CONTENT_PROGRESS = "user_content_progress";
    public static final String TABLE_CONTINUE_WATCHING = "continue_watching";
    public static final String TABLE_QUALITY_EVENTS = "playback_quality_events";

    // Query Defaults
    public static final int DEFAULT_PAGE_SIZE = 100;
    public static final int CONTINUE_WATCHING_LIMIT = 20;

    // Completion thresholds
    public static final double COMPLETION_THRESHOLD_PERCENT = 95.0;
    public static final double MIN_PROGRESS_THRESHOLD_PERCENT = 2.0;
}
