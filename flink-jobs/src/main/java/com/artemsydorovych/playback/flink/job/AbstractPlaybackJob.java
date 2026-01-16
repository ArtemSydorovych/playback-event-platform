package com.artemsydorovych.playback.flink.job;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.flink.config.FlinkConfig;
import com.artemsydorovych.playback.flink.config.JobParameters;
import com.artemsydorovych.playback.flink.function.EventTimestampAssigner;
import com.artemsydorovych.playback.flink.source.PlaybackEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for all Flink jobs in the playback platform.
 *
 * Netflix-style production patterns:
 * - RocksDB state backend with incremental checkpoints
 * - S3-compatible checkpoint storage (MinIO for local, S3 for prod)
 * - Exponential backoff restart strategy
 * - High Availability via ZooKeeper
 * - Prometheus metrics export
 *
 * Each job runs in its own isolated container (Application Mode).
 */
public abstract class AbstractPlaybackJob {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Returns the unique name for this job.
     * Used for checkpoint paths, HA cluster-id, and metrics.
     */
    protected abstract String getJobName();

    /**
     * Returns the Kafka consumer group ID for this job.
     * Each job should have a unique consumer group.
     */
    protected abstract String getConsumerGroupId();

    /**
     * Builds the job-specific pipeline topology.
     * Called after environment and source are configured.
     */
    protected abstract void buildPipeline(
            StreamExecutionEnvironment env,
            DataStream<PlaybackEvent> eventStream,
            JobParameters params);

    /**
     * Main entry point for the job.
     * Subclasses should call this from their main() method.
     */
    protected void run(String[] args) throws Exception {
        JobParameters params = new JobParameters(args);
        log.info("Starting {} with parameters: {}", getJobName(), params);

        // Create and configure environment
        StreamExecutionEnvironment env = createEnvironment(params);

        // Create Kafka source with job-specific consumer group
        DataStream<PlaybackEvent> eventStream = createSourceStream(env, params);

        // Build job-specific pipeline
        buildPipeline(env, eventStream, params);

        // Execute
        env.execute(getJobName());
    }

    /**
     * Creates and configures the Flink execution environment.
     * Netflix-style: RocksDB state backend with S3 checkpoints.
     */
    protected StreamExecutionEnvironment createEnvironment(JobParameters params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism
        env.setParallelism(params.getParallelism());

        // Configure RocksDB state backend with incremental checkpoints
        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(stateBackend);

        // Configure checkpointing
        configureCheckpointing(env, params);

        // Configure restart strategy (exponential backoff - Netflix pattern)
        configureRestartStrategy(env);

        // Make parameters available to all operators
        env.getConfig().setGlobalJobParameters(params.toParameterTool());

        return env;
    }

    /**
     * Configures exactly-once checkpointing with S3 storage.
     */
    protected void configureCheckpointing(StreamExecutionEnvironment env, JobParameters params) {
        // Enable checkpointing
        env.enableCheckpointing(params.getCheckpointInterval().toMillis());

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // Exactly-once semantics
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // Checkpoint timeout
        checkpointConfig.setCheckpointTimeout(FlinkConfig.CHECKPOINT_TIMEOUT.toMillis());

        // Minimum pause between checkpoints
        checkpointConfig.setMinPauseBetweenCheckpoints(FlinkConfig.CHECKPOINT_MIN_PAUSE_MS);

        // Only one checkpoint at a time
        checkpointConfig.setMaxConcurrentCheckpoints(FlinkConfig.CONCURRENT_CHECKPOINTS);

        // Keep checkpoint on cancellation for recovery (Netflix pattern)
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        log.info("Checkpointing configured: interval={}ms, mode=EXACTLY_ONCE",
                params.getCheckpointInterval().toMillis());
    }

    /**
     * Configures exponential backoff restart strategy.
     * Netflix pattern: exponential delay with jitter for distributed systems.
     */
    protected void configureRestartStrategy(StreamExecutionEnvironment env) {
        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
                Time.seconds(1),      // initial backoff
                Time.minutes(1),      // max backoff
                2.0,                  // backoff multiplier
                Time.minutes(10),     // reset backoff threshold
                0.1                   // jitter factor
        ));

        log.info("Restart strategy configured: exponential delay (1s -> 1min, multiplier=2.0)");
    }

    /**
     * Creates the Kafka source stream with watermarks.
     */
    protected DataStream<PlaybackEvent> createSourceStream(
            StreamExecutionEnvironment env,
            JobParameters params) {

        // Create Kafka source with job-specific consumer group
        KafkaSource<PlaybackEvent> kafkaSource = PlaybackEventSource.createWithGroupId(
                params,
                getConsumerGroupId());

        // Create watermark strategy
        WatermarkStrategy<PlaybackEvent> watermarkStrategy = EventTimestampAssigner.create();

        // Source stream with watermarks
        return env
                .fromSource(kafkaSource, watermarkStrategy, "Kafka Source")
                .name(getJobName() + "-source")
                .uid(getJobName() + "-source");
    }
}
