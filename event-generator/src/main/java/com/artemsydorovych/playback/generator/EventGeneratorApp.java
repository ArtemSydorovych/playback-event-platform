package com.artemsydorovych.playback.generator;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application to generate and publish playback events to Kafka.
 * <p>
 * Usage:
 *   gradlew :event-generator:run
 *   gradlew :event-generator:run --args="--count 50 --delay 200"
 */
public class EventGeneratorApp {

    private static final Logger log = LoggerFactory.getLogger(EventGeneratorApp.class);

    private static final int DEFAULT_EVENT_COUNT = 100;
    private static final int DEFAULT_DELAY_MS = 100;

    public static void main(String[] args) {
        int eventCount = getIntArg(args, "--count", DEFAULT_EVENT_COUNT);
        int delayMs = getIntArg(args, "--delay", DEFAULT_DELAY_MS);

        log.info("=== Playback Event Generator ===");
        log.info("Bootstrap: {}", KafkaConfig.BOOTSTRAP_SERVERS);
        log.info("Schema Registry: {}", KafkaConfig.SCHEMA_REGISTRY_URL);
        log.info("Topic: {}", KafkaConfig.TOPIC_PLAYBACK_EVENTS);
        log.info("Events: {}, Delay: {}ms", eventCount, delayMs);

        PlaybackActivityGenerator generator = new PlaybackActivityGenerator();

        try (PlaybackEventPublisher publisher = new PlaybackEventPublisher()) {
            for (int i = 0; i < eventCount; i++) {
                PlaybackEvent event = generator.generate();
                publisher.publish(event);

                if ((i + 1) % 10 == 0) {
                    log.info("Published {} events...", i + 1);
                }

                if (delayMs > 0) {
                    Thread.sleep(delayMs);
                }
            }

            publisher.flush();
            log.info("Completed! Published: {}, Failed: {}",
                    publisher.getPublishedCount(), publisher.getFailedCount());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted");
        } catch (Exception e) {
            log.error("Error: {}", e.getMessage(), e);
        }
    }

    private static int getIntArg(String[] args, String flag, int defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                try {
                    return Integer.parseInt(args[i + 1]);
                } catch (NumberFormatException e) {
                    return defaultValue;
                }
            }
        }
        return defaultValue;
    }
}
