package com.artemsydorovych.playback.flink.sink;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.avro.PlaybackEndPayload;
import com.artemsydorovych.playback.avro.RebufferPayload;
import com.artemsydorovych.playback.cassandra.CassandraClient;
import com.artemsydorovych.playback.cassandra.EventRouter;
import com.artemsydorovych.playback.flink.config.FlinkConfig;
import com.artemsydorovych.playback.flink.metrics.PlaybackMetrics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink sink that writes raw PlaybackEvents to Cassandra.
 * Wraps the existing EventRouter for reusability.
 *
 * Exports the following Prometheus metrics:
 * - playback_cassandra_writes_total
 * - playback_cassandra_write_errors_total
 * - playback_cassandra_write_latency_ms
 * - playback_events_processed_total (by event type)
 * - playback_business_* (high-level business metrics)
 */
public class CassandraSinkFunction extends RichSinkFunction<PlaybackEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(CassandraSinkFunction.class);

    private final String contactPoints;
    private final int port;
    private final String datacenter;
    private final String keyspace;

    private transient CassandraClient client;
    private transient EventRouter router;
    private transient long processedCount;

    // Custom metrics
    private transient PlaybackMetrics.CassandraMetrics cassandraMetrics;
    private transient PlaybackMetrics.EventMetrics eventMetrics;
    private transient PlaybackMetrics.BusinessMetrics businessMetrics;

    public CassandraSinkFunction() {
        this(FlinkConfig.getEnv("cassandra.contact.points", FlinkConfig.DEFAULT_CASSANDRA_CONTACT_POINTS),
             FlinkConfig.getEnvInt("cassandra.port", FlinkConfig.DEFAULT_CASSANDRA_PORT),
             FlinkConfig.getEnv("cassandra.datacenter", FlinkConfig.DEFAULT_CASSANDRA_DATACENTER),
             FlinkConfig.getEnv("cassandra.keyspace", FlinkConfig.DEFAULT_CASSANDRA_KEYSPACE));
    }

    public CassandraSinkFunction(String contactPoints, int port, String datacenter, String keyspace) {
        this.contactPoints = contactPoints;
        this.port = port;
        this.datacenter = datacenter;
        this.keyspace = keyspace;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("Opening CassandraSinkFunction: {}:{}/{}", contactPoints, port, keyspace);

        this.client = new CassandraClient(contactPoints, port, datacenter, keyspace);
        this.router = new EventRouter();
        this.router.open(client.getSession());
        this.processedCount = 0;

        // Initialize metrics
        this.cassandraMetrics = PlaybackMetrics.createCassandraMetrics(
                getRuntimeContext().getMetricGroup(), "raw_events");
        this.eventMetrics = PlaybackMetrics.createEventMetrics(
                getRuntimeContext().getMetricGroup(), "raw-events");
        this.businessMetrics = PlaybackMetrics.createBusinessMetrics(
                getRuntimeContext().getMetricGroup());

        log.info("CassandraSinkFunction initialized with {} writers and Prometheus metrics",
            router.getWriters().size());
    }

    @Override
    public void invoke(PlaybackEvent event, Context context) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            router.route(event);
            processedCount++;

            long latency = System.currentTimeMillis() - startTime;
            cassandraMetrics.recordWrite(latency);

            // Track event type metrics
            String eventType = event.getEventType().name();
            eventMetrics.recordEventType(eventType);
            eventMetrics.processingLatencyMs.update(latency);

            // Track business metrics
            trackBusinessMetrics(event);

            if (processedCount % 1000 == 0) {
                log.info("CassandraSinkFunction processed {} events", processedCount);
            }
        } catch (Exception e) {
            cassandraMetrics.recordError();
            eventMetrics.errorsTotal.inc();
            throw e;
        }
    }

    /**
     * Track high-level business metrics from events.
     */
    private void trackBusinessMetrics(PlaybackEvent event) {
        switch (event.getEventType()) {
            case PLAY_START -> businessMetrics.recordPlaybackStart();
            case PLAYBACK_END -> {
                if (event.getPayload() instanceof PlaybackEndPayload endPayload) {
                    businessMetrics.recordPlaybackEnd(
                            endPayload.getTotalWatchTime(),
                            endPayload.getCompletionPercentage());
                }
            }
            case REBUFFER_END -> {
                if (event.getPayload() instanceof RebufferPayload rebuffer) {
                    Long duration = rebuffer.getRebufferDuration();
                    if (duration != null) {
                        businessMetrics.recordRebuffer(duration);
                    }
                }
            }
            default -> { /* other events don't have business metrics */ }
        }
    }

    @Override
    public void close() throws Exception {
        log.info("Closing CassandraSinkFunction after processing {} events", processedCount);

        if (router != null) {
            router.close();
        }
        if (client != null) {
            client.close();
        }
    }
}
