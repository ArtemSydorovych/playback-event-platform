package com.artemsydorovych.playback.flink;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.flink.config.FlinkConfig;
import com.artemsydorovych.playback.flink.config.JobParameters;
import com.artemsydorovych.playback.flink.function.ContentMetricsAggregator;
import com.artemsydorovych.playback.flink.function.EventTimestampAssigner;
import com.artemsydorovych.playback.flink.function.SessionDetector;
import com.artemsydorovych.playback.flink.schema.ContentMetrics;
import com.artemsydorovych.playback.flink.schema.UserSession;
import com.artemsydorovych.playback.flink.sink.CassandraSinkFunction;
import com.artemsydorovych.playback.flink.sink.MetricsSinkFunction;
import com.artemsydorovych.playback.flink.sink.SessionSinkFunction;
import com.artemsydorovych.playback.flink.source.PlaybackEventSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Flink job for processing playback events.
 *
 * Pipeline topology:
 * <pre>
 *                                    ┌──────────────────────────────┐
 *                                    │     Cassandra Raw Sink       │
 *                                    │   (EventRouter + Writers)    │
 *                                    └──────────────────────────────┘
 *                                                 ▲
 * ┌─────────────────┐     ┌─────────────────┐     │
 * │  Kafka Source   │────▶│   Watermark     │─────┼─────────────────────────────────┐
 * │ playback-events │     │   Assignment    │     │                                 │
 * └─────────────────┘     └─────────────────┘     │                                 │
 *                                    ┌───────────┴────────────┐     ┌──────────────┴───────────┐
 *                                    │   Key By contentId     │     │     Key By userId        │
 *                                    │   5-min Tumbling       │     │     Session Window       │
 *                                    │   Content Metrics      │     │     30-min gap           │
 *                                    └───────────┬────────────┘     └──────────────┬───────────┘
 *                                                │                                 │
 *                                    ┌───────────┴────────────┐     ┌──────────────┴───────────┐
 *                                    │  content_metrics_5min  │     │     user_sessions        │
 *                                    └────────────────────────┘     └──────────────────────────┘
 * </pre>
 */
public class PlaybackPipelineJob {

    private static final Logger log = LoggerFactory.getLogger(PlaybackPipelineJob.class);

    public static void main(String[] args) throws Exception {
        // Parse parameters
        JobParameters params = new JobParameters(args);
        log.info("Starting Playback Pipeline with parameters: {}", params);

        // Create execution environment
        StreamExecutionEnvironment env = createEnvironment(params);

        // Build pipeline
        buildPipeline(env, params);

        // Execute
        env.execute(FlinkConfig.JOB_NAME);
    }

    /**
     * Creates and configures the Flink execution environment.
     * Netflix-style: RocksDB state backend with exactly-once checkpointing.
     */
    public static StreamExecutionEnvironment createEnvironment(JobParameters params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set parallelism
        env.setParallelism(params.getParallelism());

        // Configure RocksDB state backend
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // Configure exactly-once checkpointing
        configureCheckpointing(env, params);

        // Make parameters available to all operators
        env.getConfig().setGlobalJobParameters(params.toParameterTool());

        return env;
    }

    /**
     * Configures exactly-once checkpointing (Netflix production standard).
     */
    private static void configureCheckpointing(StreamExecutionEnvironment env, JobParameters params) {
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

        // Keep checkpoint on cancellation for recovery
        checkpointConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        log.info("Checkpointing configured: interval={}ms, mode=EXACTLY_ONCE",
            params.getCheckpointInterval().toMillis());
    }

    /**
     * Builds the three-branch processing pipeline.
     */
    public static void buildPipeline(StreamExecutionEnvironment env, JobParameters params) {
        // Create Kafka source
        KafkaSource<PlaybackEvent> kafkaSource = PlaybackEventSource.create(params);

        // Create watermark strategy
        WatermarkStrategy<PlaybackEvent> watermarkStrategy = EventTimestampAssigner.create();

        // Source stream with watermarks
        DataStream<PlaybackEvent> eventStream = env
            .fromSource(kafkaSource, watermarkStrategy, "Kafka Source")
            .name("playback-events-source")
            .uid("playback-events-source");

        // Branch 1: Raw events to Cassandra
        if (params.isRawSinkEnabled()) {
            buildRawEventSink(eventStream, params);
        }

        // Branch 2: Content metrics aggregation
        if (params.isMetricsSinkEnabled()) {
            buildContentMetricsPipeline(eventStream, params);
        }

        // Branch 3: Session detection
        if (params.isSessionSinkEnabled()) {
            buildSessionPipeline(eventStream, params);
        }

        log.info("Pipeline built with: rawSink={}, metricsSink={}, sessionSink={}",
            params.isRawSinkEnabled(),
            params.isMetricsSinkEnabled(),
            params.isSessionSinkEnabled());
    }

    /**
     * Branch 1: Write raw events to Cassandra via EventRouter.
     */
    private static void buildRawEventSink(DataStream<PlaybackEvent> eventStream, JobParameters params) {
        eventStream
            .addSink(new CassandraSinkFunction(
                params.getCassandraContactPoints(),
                params.getCassandraPort(),
                params.getCassandraDatacenter(),
                params.getCassandraKeyspace()))
            .name("Cassandra Raw Sink")
            .uid("cassandra-raw-sink")
            .setParallelism(params.getSinkParallelism());

        log.info("Raw event sink configured");
    }

    /**
     * Branch 2: Aggregate events into 5-minute content metrics.
     */
    private static void buildContentMetricsPipeline(
            DataStream<PlaybackEvent> eventStream,
            JobParameters params) {

        SingleOutputStreamOperator<ContentMetrics> metricsStream = eventStream
            .keyBy(event -> event.getContentId().toString())
            .window(TumblingEventTimeWindows.of(
                Time.milliseconds(FlinkConfig.CONTENT_METRICS_WINDOW.toMillis())))
            .allowedLateness(Time.milliseconds(FlinkConfig.ALLOWED_LATENESS.toMillis()))
            .aggregate(
                ContentMetricsAggregator.aggregateFunction(),
                ContentMetricsAggregator.windowFunction())
            .name("Content Metrics Aggregation")
            .uid("content-metrics-aggregation");

        metricsStream
            .addSink(new MetricsSinkFunction(
                params.getCassandraContactPoints(),
                params.getCassandraPort(),
                params.getCassandraDatacenter(),
                params.getCassandraKeyspace()))
            .name("Cassandra Metrics Sink")
            .uid("cassandra-metrics-sink")
            .setParallelism(params.getSinkParallelism());

        log.info("Content metrics pipeline configured with 5-minute windows");
    }

    /**
     * Branch 3: Detect user sessions via 30-minute inactivity gaps.
     */
    private static void buildSessionPipeline(
            DataStream<PlaybackEvent> eventStream,
            JobParameters params) {

        SingleOutputStreamOperator<UserSession> sessionStream = eventStream
            .keyBy(event -> event.getUserId().toString())
            .process(new SessionDetector())
            .name("Session Detection")
            .uid("session-detection");

        sessionStream
            .addSink(new SessionSinkFunction(
                params.getCassandraContactPoints(),
                params.getCassandraPort(),
                params.getCassandraDatacenter(),
                params.getCassandraKeyspace()))
            .name("Cassandra Session Sink")
            .uid("cassandra-session-sink")
            .setParallelism(params.getSinkParallelism());

        log.info("Session detection pipeline configured with 30-minute gap");
    }
}
