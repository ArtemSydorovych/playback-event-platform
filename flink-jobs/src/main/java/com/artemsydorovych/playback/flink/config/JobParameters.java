package com.artemsydorovych.playback.flink.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.time.Duration;

/**
 * Production-style configuration management.
 *
 * Priority order (highest to lowest):
 * 1. Command-line arguments: --kafka.bootstrap.servers=host:9092
 * 2. Environment variables: FLINK_KAFKA_BOOTSTRAP_SERVERS=host:9092
 * 3. Default values from FlinkConfig
 *
 * Usage:
 *   Local dev:  java -jar job.jar (uses localhost defaults)
 *   Docker:     Set env vars in docker-compose.yml
 *   K8s:        Set env vars in deployment.yaml or ConfigMap
 */
public class JobParameters {

    private final ParameterTool params;

    public JobParameters(String[] args) {
        this.params = ParameterTool.fromArgs(args);
    }

    public JobParameters(ParameterTool params) {
        this.params = params;
    }

    // ==================== Kafka Configuration ====================

    public String getKafkaBootstrapServers() {
        return resolve("kafka.bootstrap.servers", FlinkConfig.DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
    }

    public String getKafkaTopic() {
        return resolve("kafka.topic", FlinkConfig.DEFAULT_KAFKA_TOPIC);
    }

    public String getKafkaGroupId() {
        return resolve("kafka.group.id", FlinkConfig.DEFAULT_KAFKA_GROUP_ID);
    }

    public String getSchemaRegistryUrl() {
        return resolve("schema.registry.url", FlinkConfig.DEFAULT_SCHEMA_REGISTRY_URL);
    }

    // ==================== Cassandra Configuration ====================

    public String getCassandraContactPoints() {
        return resolve("cassandra.contact.points", FlinkConfig.DEFAULT_CASSANDRA_CONTACT_POINTS);
    }

    public int getCassandraPort() {
        return resolveInt("cassandra.port", FlinkConfig.DEFAULT_CASSANDRA_PORT);
    }

    public String getCassandraDatacenter() {
        return resolve("cassandra.datacenter", FlinkConfig.DEFAULT_CASSANDRA_DATACENTER);
    }

    public String getCassandraKeyspace() {
        return resolve("cassandra.keyspace", FlinkConfig.DEFAULT_CASSANDRA_KEYSPACE);
    }

    // ==================== Checkpointing ====================

    public Duration getCheckpointInterval() {
        long seconds = resolveLong("checkpoint.interval.seconds",
            FlinkConfig.CHECKPOINT_INTERVAL.toSeconds());
        return Duration.ofSeconds(seconds);
    }

    // ==================== Parallelism ====================

    public int getParallelism() {
        return resolveInt("parallelism", FlinkConfig.DEFAULT_PARALLELISM);
    }

    public int getSinkParallelism() {
        return resolveInt("sink.parallelism", FlinkConfig.DEFAULT_SINK_PARALLELISM);
    }

    // ==================== Feature Flags ====================

    public boolean isRawSinkEnabled() {
        return resolveBoolean("sink.raw.enabled", true);
    }

    public boolean isMetricsSinkEnabled() {
        return resolveBoolean("sink.metrics.enabled", true);
    }

    public boolean isSessionSinkEnabled() {
        return resolveBoolean("sink.sessions.enabled", true);
    }

    // ==================== Resolution Methods ====================

    /**
     * Resolve config value with priority: CLI > ENV > default
     */
    private String resolve(String key, String defaultValue) {
        // 1. Check CLI args first
        if (params.has(key)) {
            return params.get(key);
        }
        // 2. Check environment variable
        return FlinkConfig.getEnv(key, defaultValue);
    }

    private int resolveInt(String key, int defaultValue) {
        if (params.has(key)) {
            return params.getInt(key, defaultValue);
        }
        return FlinkConfig.getEnvInt(key, defaultValue);
    }

    private long resolveLong(String key, long defaultValue) {
        if (params.has(key)) {
            return params.getLong(key, defaultValue);
        }
        String value = FlinkConfig.getEnv(key, null);
        if (value == null) return defaultValue;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private boolean resolveBoolean(String key, boolean defaultValue) {
        if (params.has(key)) {
            return params.getBoolean(key, defaultValue);
        }
        String value = FlinkConfig.getEnv(key, null);
        if (value == null) return defaultValue;
        return Boolean.parseBoolean(value);
    }

    // ==================== Utility ====================

    public ParameterTool toParameterTool() {
        return params;
    }

    @Override
    public String toString() {
        return String.format(
            "JobParameters[kafka=%s, schemaRegistry=%s, cassandra=%s:%d/%s, parallelism=%d]",
            getKafkaBootstrapServers(),
            getSchemaRegistryUrl(),
            getCassandraContactPoints(),
            getCassandraPort(),
            getCassandraKeyspace(),
            getParallelism()
        );
    }
}
