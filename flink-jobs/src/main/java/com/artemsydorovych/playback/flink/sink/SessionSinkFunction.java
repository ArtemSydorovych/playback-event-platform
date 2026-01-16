package com.artemsydorovych.playback.flink.sink;

import com.artemsydorovych.playback.cassandra.CassandraClient;
import com.artemsydorovych.playback.flink.config.FlinkConfig;
import com.artemsydorovych.playback.flink.metrics.PlaybackMetrics;
import com.artemsydorovych.playback.flink.schema.UserSession;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Flink sink that writes UserSession aggregations to Cassandra.
 *
 * Exports Prometheus metrics:
 * - playback_cassandra_writes_total (table=user_sessions)
 * - playback_cassandra_write_errors_total
 * - playback_cassandra_write_latency_ms
 */
public class SessionSinkFunction extends RichSinkFunction<UserSession> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(SessionSinkFunction.class);

    private static final String INSERT_CQL = """
        INSERT INTO user_sessions (
            user_id, session_start, session_id,
            device_type, session_duration_ms,
            content_count, total_events,
            total_watch_time_ms, avg_completion_percentage,
            play_start_count, pause_count, seek_count, error_count,
            rebuffer_count, total_rebuffer_duration_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    public SessionSinkFunction() {
        this(FlinkConfig.getEnv("cassandra.contact.points", FlinkConfig.DEFAULT_CASSANDRA_CONTACT_POINTS),
             FlinkConfig.getEnvInt("cassandra.port", FlinkConfig.DEFAULT_CASSANDRA_PORT),
             FlinkConfig.getEnv("cassandra.datacenter", FlinkConfig.DEFAULT_CASSANDRA_DATACENTER),
             FlinkConfig.getEnv("cassandra.keyspace", FlinkConfig.DEFAULT_CASSANDRA_KEYSPACE));
    }

    public SessionSinkFunction(String contactPoints, int port, String datacenter, String keyspace) {
        this.contactPoints = contactPoints;
        this.port = port;
        this.datacenter = datacenter;
        this.keyspace = keyspace;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("Opening SessionSinkFunction: {}:{}/{}", contactPoints, port, keyspace);

        this.client = new CassandraClient(contactPoints, port, datacenter, keyspace);
        this.session = client.getSession();
        this.preparedStatement = session.prepare(INSERT_CQL);
        this.processedCount = 0;

        // Initialize Prometheus metrics
        this.cassandraMetrics = PlaybackMetrics.createCassandraMetrics(
                getRuntimeContext().getMetricGroup(), "user_sessions");

        log.info("SessionSinkFunction initialized with Prometheus metrics");
    }

    @Override
    public void invoke(UserSession userSession, Context context) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            session.execute(preparedStatement.bind(
                userSession.getUserId(),
                toInstant(userSession.getSessionStart()),
                userSession.getSessionId(),
                userSession.getDeviceType(),
                userSession.getSessionDurationMs(),
                userSession.getContentCount(),
                userSession.getTotalEvents(),
                userSession.getTotalWatchTimeMs(),
                userSession.getAvgCompletionPercentage(),
                userSession.getPlayStartCount(),
                userSession.getPauseCount(),
                userSession.getSeekCount(),
                userSession.getErrorCount(),
                userSession.getRebufferCount(),
                userSession.getTotalRebufferDurationMs()
            ));

            long latency = System.currentTimeMillis() - startTime;
            cassandraMetrics.recordWrite(latency);
            processedCount++;

            log.info("SessionSinkFunction: wrote session user={}, session={}, duration={}ms, events={}, latency={}ms",
                userSession.getUserId(),
                userSession.getSessionId(),
                userSession.getSessionDurationMs(),
                userSession.getTotalEvents(),
                latency);
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
        log.info("Closing SessionSinkFunction after processing {} sessions", processedCount);

        if (client != null) {
            client.close();
        }
    }
}
