package com.artemsydorovych.playback.flink.job;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.flink.config.FlinkConfig;
import com.artemsydorovych.playback.flink.config.JobParameters;
import com.artemsydorovych.playback.flink.function.ContentMetricsAggregator;
import com.artemsydorovych.playback.flink.schema.ContentMetrics;
import com.artemsydorovych.playback.flink.sink.MetricsSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Content Metrics Job - Aggregates playback events into 5-minute content metrics.
 *
 * Netflix-style: One job per container with dedicated Kafka consumer group.
 *
 * Pipeline:
 * <pre>
 *   Kafka (playback-events)
 *          │
 *          ▼
 *   KeyBy(contentId)
 *          │
 *          ▼
 *   TumblingEventTimeWindows(5 min)
 *          │
 *          ▼
 *   ContentMetricsAggregator
 *          │
 *          ▼
 *   content_metrics_5min (Cassandra)
 * </pre>
 *
 * Aggregated metrics per window:
 * - View count, unique viewers
 * - Total watch time
 * - Rebuffer events, errors
 * - Device breakdown (mobile, tv, web, other)
 * - Average bitrate, completion percentage
 */
public class ContentMetricsJob extends AbstractPlaybackJob {

    private static final String JOB_NAME = "playback-content-metrics";
    private static final String CONSUMER_GROUP = "flink-content-metrics-consumer";

    public static void main(String[] args) throws Exception {
        new ContentMetricsJob().run(args);
    }

    @Override
    protected String getJobName() {
        return JOB_NAME;
    }

    @Override
    protected String getConsumerGroupId() {
        return CONSUMER_GROUP;
    }

    @Override
    protected void buildPipeline(
            StreamExecutionEnvironment env,
            DataStream<PlaybackEvent> eventStream,
            JobParameters params) {

        // Aggregate into 5-minute content metrics
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

        // Write to Cassandra
        metricsStream
            .addSink(new MetricsSinkFunction(
                params.getCassandraContactPoints(),
                params.getCassandraPort(),
                params.getCassandraDatacenter(),
                params.getCassandraKeyspace()))
            .name("Cassandra Metrics Sink")
            .uid("cassandra-metrics-sink")
            .setParallelism(params.getSinkParallelism());

        log.info("Content metrics pipeline configured: {} window, {} lateness",
            FlinkConfig.CONTENT_METRICS_WINDOW,
            FlinkConfig.ALLOWED_LATENESS);
    }
}
