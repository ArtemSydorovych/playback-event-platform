package com.artemsydorovych.playback.flink.sink;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.cassandra.CassandraClient;
import com.artemsydorovych.playback.cassandra.EventRouter;
import com.artemsydorovych.playback.flink.config.FlinkConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink sink that writes raw PlaybackEvents to Cassandra.
 * Wraps the existing EventRouter for reusability.
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

        log.info("CassandraSinkFunction initialized with {} writers",
            router.getWriters().size());
    }

    @Override
    public void invoke(PlaybackEvent event, Context context) throws Exception {
        router.route(event);
        processedCount++;

        if (processedCount % 1000 == 0) {
            log.info("CassandraSinkFunction processed {} events", processedCount);
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
