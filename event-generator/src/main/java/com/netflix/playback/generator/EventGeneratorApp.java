package com.netflix.playback.generator;

import com.netflix.playback.model.PlaybackEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application to generate and send playback events to Kafka.
 */
public class EventGeneratorApp {

    private static final Logger log = LoggerFactory.getLogger(EventGeneratorApp.class);

    private static final String BOOTSTRAP_SERVERS = System.getenv()
        .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String TOPIC = System.getenv()
        .getOrDefault("KAFKA_TOPIC", "playback-events");
    private static final int EVENT_COUNT = Integer.parseInt(System.getenv()
        .getOrDefault("EVENT_COUNT", "100"));
    private static final int DELAY_MS = Integer.parseInt(System.getenv()
        .getOrDefault("DELAY_MS", "100"));

    public static void main(String[] args) {
        log.info("Starting Event Generator");
        log.info("Bootstrap servers: {}", BOOTSTRAP_SERVERS);
        log.info("Topic: {}", TOPIC);
        log.info("Event count: {}", EVENT_COUNT);
        log.info("Delay between events: {}ms", DELAY_MS);

        try (SimpleEventProducer producer = new SimpleEventProducer(BOOTSTRAP_SERVERS, TOPIC)) {
            for (int i = 0; i < EVENT_COUNT; i++) {
                PlaybackEvent event = EventSimulator.generateRandomEvent();
                producer.send(event);

                if ((i + 1) % 10 == 0) {
                    log.info("Sent {} events...", i + 1);
                }

                if (DELAY_MS > 0) {
                    Thread.sleep(DELAY_MS);
                }
            }

            producer.flush();
            log.info("Completed! Sent: {}, Errors: {}", producer.getSentCount(), producer.getErrorCount());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted");
        } catch (Exception e) {
            log.error("Error: {}", e.getMessage(), e);
        }
    }
}
