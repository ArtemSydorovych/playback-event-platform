package com.artemsydorovych.playback.cassandra;

import com.artemsydorovych.playback.config.CassandraConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Manages Cassandra connection lifecycle.
 * Thread-safe singleton pattern for session management.
 */
public class CassandraClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CassandraClient.class);

    private final CqlSession session;

    /**
     * Create a client with default configuration.
     */
    public CassandraClient() {
        this(CassandraConfig.CONTACT_POINT, CassandraConfig.PORT,
                CassandraConfig.LOCAL_DATACENTER, CassandraConfig.KEYSPACE);
    }

    /**
     * Create a client with custom configuration.
     */
    public CassandraClient(String contactPoint, int port, String datacenter, String keyspace) {
        log.info("Connecting to Cassandra at {}:{}, keyspace: {}", contactPoint, port, keyspace);

        CqlSessionBuilder builder = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withLocalDatacenter(datacenter);

        if (keyspace != null && !keyspace.isEmpty()) {
            builder.withKeyspace(keyspace);
        }

        this.session = builder.build();
        log.info("Connected to Cassandra cluster: {}", session.getMetadata().getClusterName().orElse("unknown"));
    }

    /**
     * Get the CQL session for executing queries.
     */
    public CqlSession getSession() {
        return session;
    }

    /**
     * Check if the connection is active.
     */
    public boolean isConnected() {
        return session != null && !session.isClosed();
    }

    @Override
    public void close() {
        if (session != null && !session.isClosed()) {
            log.info("Closing Cassandra connection");
            session.close();
        }
    }
}
