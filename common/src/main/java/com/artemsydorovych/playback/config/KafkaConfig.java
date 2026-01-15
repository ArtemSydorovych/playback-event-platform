package com.artemsydorovych.playback.config;

/**
 * Centralized configuration constants for Kafka and Schema Registry.
 */
public final class KafkaConfig {

    private KafkaConfig() {
        // Prevent instantiation
    }

    // Kafka Bootstrap Servers
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    // Schema Registry
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    // Topics
    public static final String TOPIC_PLAYBACK_EVENTS = "playback-events";
    public static final String TOPIC_PLAYBACK_EVENTS_DLQ = "playback-events-dlq";

    // Consumer Groups
    public static final String CONSUMER_GROUP_CONSOLE = "console-consumer-group";
    public static final String CONSUMER_GROUP_ANALYTICS = "analytics-consumer-group";

    // Default Producer Settings
    public static final String DEFAULT_ACKS = "1";
    public static final int DEFAULT_RETRIES = 3;
    public static final int DEFAULT_LINGER_MS = 5;
    public static final int DEFAULT_BATCH_SIZE = 16384;

    // Default Consumer Settings
    public static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";
    public static final int DEFAULT_AUTO_COMMIT_INTERVAL_MS = 1000;
}
