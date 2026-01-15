package com.artemsydorovych.playback.cassandra;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.consumer.PlaybackEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumes playback events from Kafka and writes to Cassandra.
 * Standalone application for running outside of Flink.
 */
public class CassandraConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(CassandraConsumerApp.class);

    private final CassandraClient cassandraClient;
    private final EventRouter eventRouter;
    private final PlaybackEventProcessor kafkaProcessor;
    private final AtomicLong eventCount = new AtomicLong(0);

    public CassandraConsumerApp() {
        this.cassandraClient = new CassandraClient();
        this.eventRouter = new EventRouter();
        this.kafkaProcessor = new PlaybackEventProcessor();
    }

    public void start() {
        log.info("Starting Cassandra Consumer...");

        // Initialize Cassandra writers
        eventRouter.open(cassandraClient.getSession());

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        log.info("Connected to Cassandra. Starting Kafka consumer...");

        // Start consuming from Kafka
        kafkaProcessor.process(this::handleEvent);
    }

    private void handleEvent(PlaybackEvent event) {
        eventRouter.route(event);

        long count = eventCount.incrementAndGet();
        if (count % 100 == 0) {
            log.info("Processed {} events", count);
        }

        if (log.isDebugEnabled()) {
            log.debug("Wrote event: {} [{}] user={} content={}",
                event.getEventId(),
                event.getEventType(),
                event.getUserId(),
                event.getContentId()
            );
        }
    }

    private void shutdown() {
        log.info("Shutting down...");

        try {
            kafkaProcessor.close();
        } catch (Exception e) {
            log.warn("Error closing Kafka processor: {}", e.getMessage());
        }

        try {
            eventRouter.close();
        } catch (Exception e) {
            log.warn("Error closing event router: {}", e.getMessage());
        }

        try {
            cassandraClient.close();
        } catch (Exception e) {
            log.warn("Error closing Cassandra client: {}", e.getMessage());
        }

        log.info("Shutdown complete. Total events processed: {}", eventCount.get());
    }

    public static void main(String[] args) {
        new CassandraConsumerApp().start();
    }
}
