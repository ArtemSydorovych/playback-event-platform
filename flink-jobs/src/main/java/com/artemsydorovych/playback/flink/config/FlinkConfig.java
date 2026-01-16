package com.artemsydorovych.playback.flink.config;

import java.time.Duration;

/**
 * Configuration constants for Flink jobs.
 *
 * Production pattern: All values can be overridden via:
 * 1. Environment variables (highest priority)
 * 2. Command-line arguments (--kafka.bootstrap.servers=...)
 * 3. Default values (lowest priority)
 */
public final class FlinkConfig {

    private FlinkConfig() {}

    // Job names
    public static final String JOB_NAME = "playback-event-pipeline";

    // Environment variable prefixes
    public static final String ENV_PREFIX = "FLINK_";

    // Kafka defaults (overridable via env: FLINK_KAFKA_BOOTSTRAP_SERVERS)
    public static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_KAFKA_TOPIC = "playback-events";
    public static final String DEFAULT_KAFKA_GROUP_ID = "flink-playback-consumer";
    public static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

    // Cassandra defaults (overridable via env: FLINK_CASSANDRA_CONTACT_POINTS)
    public static final String DEFAULT_CASSANDRA_CONTACT_POINTS = "localhost";
    public static final int DEFAULT_CASSANDRA_PORT = 9042;
    public static final String DEFAULT_CASSANDRA_DATACENTER = "datacenter1";
    public static final String DEFAULT_CASSANDRA_KEYSPACE = "playback";

    // Checkpointing
    public static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(60);
    public static final Duration CHECKPOINT_TIMEOUT = Duration.ofMinutes(10);
    public static final int CHECKPOINT_MIN_PAUSE_MS = 500;
    public static final int CONCURRENT_CHECKPOINTS = 1;

    // Watermarks
    public static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(30);
    public static final Duration IDLE_TIMEOUT = Duration.ofMinutes(1);

    // Windows
    public static final Duration CONTENT_METRICS_WINDOW = Duration.ofMinutes(5);
    public static final Duration ALLOWED_LATENESS = Duration.ofMinutes(1);

    // Session detection
    public static final Duration SESSION_GAP = Duration.ofMinutes(30);
    public static final Duration SESSION_STATE_TTL = Duration.ofHours(24);

    // State TTL
    public static final Duration METRICS_STATE_TTL = Duration.ofHours(1);

    // Parallelism defaults
    public static final int DEFAULT_PARALLELISM = 4;
    public static final int DEFAULT_SINK_PARALLELISM = 2;

    /**
     * Get config value from environment variable with fallback.
     * Env var format: FLINK_KAFKA_BOOTSTRAP_SERVERS
     */
    public static String getEnv(String key, String defaultValue) {
        String envKey = ENV_PREFIX + key.toUpperCase().replace(".", "_").replace("-", "_");
        String value = System.getenv(envKey);
        return value != null && !value.isEmpty() ? value : defaultValue;
    }

    public static int getEnvInt(String key, int defaultValue) {
        String value = getEnv(key, null);
        if (value == null) return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
