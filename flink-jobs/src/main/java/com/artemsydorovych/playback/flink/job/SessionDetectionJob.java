package com.artemsydorovych.playback.flink.job;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.flink.config.FlinkConfig;
import com.artemsydorovych.playback.flink.config.JobParameters;
import com.artemsydorovych.playback.flink.function.SessionDetector;
import com.artemsydorovych.playback.flink.schema.UserSession;
import com.artemsydorovych.playback.flink.sink.SessionSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Session Detection Job - Detects user viewing sessions based on 30-minute inactivity gaps.
 *
 * Production deployment: One job per container with dedicated Kafka consumer group.
 *
 * Pipeline:
 * <pre>
 *   Kafka (playback-events)
 *          │
 *          ▼
 *   KeyBy(userId)
 *          │
 *          ▼
 *   SessionDetector (KeyedProcessFunction)
 *   - Maintains session state per user
 *   - Event-time timers for gap detection
 *   - 30-minute inactivity = session end
 *   - State TTL: 24 hours
 *          │
 *          ▼
 *   user_sessions (Cassandra)
 * </pre>
 *
 * Session data captured:
 * - Session ID, user ID
 * - Start/end timestamps, duration
 * - Content watched (list of content IDs)
 * - Event counts (play, pause, seek, etc.)
 * - Watch time, completion percentage
 * - Device information
 */
public class SessionDetectionJob extends AbstractPlaybackJob {

    private static final String JOB_NAME = "playback-session-detection";
    private static final String CONSUMER_GROUP = "flink-session-detection-consumer";

    public static void main(String[] args) throws Exception {
        new SessionDetectionJob().run(args);
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

        // Detect sessions via inactivity gaps
        SingleOutputStreamOperator<UserSession> sessionStream = eventStream
            .keyBy(event -> event.getUserId().toString())
            .process(new SessionDetector())
            .name("Session Detection")
            .uid("session-detection");

        // Write to Cassandra
        sessionStream
            .addSink(new SessionSinkFunction(
                params.getCassandraContactPoints(),
                params.getCassandraPort(),
                params.getCassandraDatacenter(),
                params.getCassandraKeyspace()))
            .name("Cassandra Session Sink")
            .uid("cassandra-session-sink")
            .setParallelism(params.getSinkParallelism());

        log.info("Session detection pipeline configured: {} gap, {} state TTL",
            FlinkConfig.SESSION_GAP,
            FlinkConfig.SESSION_STATE_TTL);
    }
}
