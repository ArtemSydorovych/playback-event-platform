package com.artemsydorovych.playback.flink.sink;

import com.artemsydorovych.playback.cassandra.CassandraClient;
import com.artemsydorovych.playback.flink.config.FlinkConfig;
import com.artemsydorovych.playback.flink.metrics.PlaybackMetrics;
import com.artemsydorovych.playback.flink.schema.ContentMetrics;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Flink sink that writes ContentMetrics aggregations to Cassandra.
 *
 * Exports Prometheus metrics:
 * - playback_cassandra_writes_total (table=content_metrics_5min)
 * - playback_cassandra_write_errors_total
 * - playback_cassandra_write_latency_ms
 */
public class MetricsSinkFunction extends RichSinkFunction<ContentMetrics> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(MetricsSinkFunction.class);

    private static final String INSERT_CQL = """
        INSERT INTO content_metrics_5min (
            content_id, window_start, window_end,
            view_count, unique_viewers, completed_views,
            total_watch_time_ms, avg_watch_time_ms,
            rebuffer_count, total_rebuffer_duration_ms,
            error_count, pause_count, seek_count, quality_change_count,
            mobile_views, desktop_views, tv_views, other_device_views
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

    private final String contactPoints;
    private final int port;
    private final String datacenter;
    private final String keyspace;

    private transient CassandraClient client;
    private transient CqlSession session;
    private transient PreparedStatement preparedStatement;
    private transient long processedCount;
    private transient PlaybackMetrics.CassandraMetrics cassandraMetrics;

    public MetricsSinkFunction() {
        this(FlinkConfig.getEnv("cassandra.contact.points", FlinkConfig.DEFAULT_CASSANDRA_CONTACT_POINTS),
             FlinkConfig.getEnvInt("cassandra.port", FlinkConfig.DEFAULT_CASSANDRA_PORT),
             FlinkConfig.getEnv("cassandra.datacenter", FlinkConfig.DEFAULT_CASSANDRA_DATACENTER),
             FlinkConfig.getEnv("cassandra.keyspace", FlinkConfig.DEFAULT_CASSANDRA_KEYSPACE));
    }

    public MetricsSinkFunction(String contactPoints, int port, String datacenter, String keyspace) {
        this.contactPoints = contactPoints;
        this.port = port;
        this.datacenter = datacenter;
        this.keyspace = keyspace;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("Opening MetricsSinkFunction: {}:{}/{}", contactPoints, port, keyspace);

        this.client = new CassandraClient(contactPoints, port, datacenter, keyspace);
        this.session = client.getSession();
        this.preparedStatement = session.prepare(INSERT_CQL);
        this.processedCount = 0;

        // Initialize Prometheus metrics
        this.cassandraMetrics = PlaybackMetrics.createCassandraMetrics(
                getRuntimeContext().getMetricGroup(), "content_metrics_5min");

        log.info("MetricsSinkFunction initialized with Prometheus metrics");
    }

    @Override
    public void invoke(ContentMetrics metrics, Context context) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            session.execute(preparedStatement.bind(
                metrics.getContentId(),
                toInstant(metrics.getWindowStart()),
                toInstant(metrics.getWindowEnd()),
                metrics.getViewCount(),
                metrics.getUniqueViewers(),
                metrics.getCompletedViews(),
                metrics.getTotalWatchTimeMs(),
                metrics.getAvgWatchTimeMs(),
                metrics.getRebufferCount(),
                metrics.getTotalRebufferDurationMs(),
                metrics.getErrorCount(),
                metrics.getPauseCount(),
                metrics.getSeekCount(),
                metrics.getQualityChangeCount(),
                metrics.getMobileViews(),
                metrics.getDesktopViews(),
                metrics.getTvViews(),
                metrics.getOtherDeviceViews()
            ));

            long latency = System.currentTimeMillis() - startTime;
            cassandraMetrics.recordWrite(latency);
            processedCount++;

            if (log.isDebugEnabled() || processedCount % 100 == 0) {
                log.info("MetricsSinkFunction: wrote metrics for content={}, window={}-{}, views={}, latency={}ms",
                    metrics.getContentId(),
                    metrics.getWindowStart(),
                    metrics.getWindowEnd(),
                    metrics.getViewCount(),
                    latency);
            }
        } catch (Exception e) {
            cassandraMetrics.recordError();
            throw e;
        }
    }

    private Instant toInstant(Instant instant) {
        return instant;
    }

    @Override
    public void close() throws Exception {
        log.info("Closing MetricsSinkFunction after processing {} metrics", processedCount);

        if (client != null) {
            client.close();
        }
    }
}
