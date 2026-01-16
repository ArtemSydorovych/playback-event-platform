package com.artemsydorovych.playback.flink.job;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.flink.config.JobParameters;
import com.artemsydorovych.playback.flink.sink.CassandraSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Raw Events Job - Writes all playback events to Cassandra.
 *
 * Netflix-style: One job per container with dedicated Kafka consumer group.
 *
 * Pipeline:
 * <pre>
 *   Kafka (playback-events)
 *          │
 *          ▼
 *   CassandraSinkFunction (EventRouter)
 *          │
 *          ▼
 *   ┌──────────────────────────────┐
 *   │  events_by_user              │
 *   │  events_by_session           │
 *   │  events_by_content           │
 *   │  user_content_progress       │
 *   │  continue_watching           │
 *   │  playback_quality_events     │
 *   └──────────────────────────────┘
 * </pre>
 */
public class RawEventsJob extends AbstractPlaybackJob {

    private static final String JOB_NAME = "playback-raw-events";
    private static final String CONSUMER_GROUP = "flink-raw-events-consumer";

    public static void main(String[] args) throws Exception {
        new RawEventsJob().run(args);
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

        eventStream
            .addSink(new CassandraSinkFunction(
                params.getCassandraContactPoints(),
                params.getCassandraPort(),
                params.getCassandraDatacenter(),
                params.getCassandraKeyspace()))
            .name("Cassandra Raw Sink")
            .uid("cassandra-raw-sink")
            .setParallelism(params.getSinkParallelism());

        log.info("Raw events pipeline configured: writing to {} Cassandra tables",
            "6 (events_by_user, events_by_session, events_by_content, " +
            "user_content_progress, continue_watching, playback_quality_events)");
    }
}
