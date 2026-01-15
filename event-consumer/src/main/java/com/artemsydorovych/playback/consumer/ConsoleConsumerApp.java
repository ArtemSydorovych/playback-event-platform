package com.artemsydorovych.playback.consumer;

import com.artemsydorovych.playback.avro.*;
import com.artemsydorovych.playback.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application that consumes playback events and prints them to console.
 * <p>
 * Usage:
 *   gradlew :event-consumer:run
 */
public class ConsoleConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(ConsoleConsumerApp.class);

    public static void main(String[] args) {
        log.info("=== Playback Event Consumer ===");
        log.info("Bootstrap: {}", KafkaConfig.BOOTSTRAP_SERVERS);
        log.info("Schema Registry: {}", KafkaConfig.SCHEMA_REGISTRY_URL);
        log.info("Topic: {}", KafkaConfig.TOPIC_PLAYBACK_EVENTS);
        log.info("Group: {}", KafkaConfig.CONSUMER_GROUP_CONSOLE);

        try (PlaybackEventProcessor processor = new PlaybackEventProcessor()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received...");
                processor.stop();
            }));

            processor.process(ConsoleConsumerApp::printEvent);

        } catch (Exception e) {
            log.error("Error: {}", e.getMessage(), e);
        }
    }

    private static void printEvent(PlaybackEvent event) {
        System.out.println();
        System.out.println("===============================================================");
        System.out.printf(" Event: %-15s | User: %-10s | Content: %s%n",
                event.getEventType(),
                event.getUserId(),
                event.getContentId());
        System.out.println("---------------------------------------------------------------");

        Long position = event.getPosition();
        Long duration = event.getDuration();
        double completion = (position != null && duration != null && duration > 0)
                ? (position * 100.0 / duration) : 0;

        System.out.printf(" Position: %s | Progress: %.1f%%%n",
                position != null ? formatMs(position) : "N/A",
                completion);

        DeviceInfo deviceInfo = event.getDeviceInfo();
        System.out.printf(" Device: %-12s | Country: %s%n",
                deviceInfo != null ? deviceInfo.getDeviceType() : "N/A",
                deviceInfo != null ? deviceInfo.getCountry() : "N/A");
        System.out.printf(" Session: %s%n", event.getSessionId());
        System.out.printf(" Time: %s%n", event.getTimestamp());

        Object payload = event.getPayload();
        if (payload != null) {
            System.out.println("---------------------------------------------------------------");
            printPayload(payload);
        }
        System.out.println("===============================================================");
    }

    private static void printPayload(Object payload) {
        if (payload instanceof SeekPayload seek) {
            long from = seek.getFromPosition();
            long to = seek.getToPosition();
            String direction = to > from ? "forward" : "backward";
            System.out.printf(" Seek: %s -> %s (%s)%n",
                    formatMs(from), formatMs(to), direction);
        } else if (payload instanceof ProgressPayload progress) {
            System.out.printf(" Quality: %s @ %d kbps | Buffer: %dms%n",
                    progress.getCurrentQuality(),
                    progress.getCurrentBitrate(),
                    progress.getBufferLength());
        } else if (payload instanceof PlaybackEndPayload end) {
            System.out.printf(" End Reason: %s | Watched: %s (%.1f%%)%n",
                    end.getReason(),
                    formatMs(end.getTotalWatchTime()),
                    end.getCompletionPercentage());
        } else if (payload instanceof QualityChangePayload qc) {
            System.out.printf(" Quality: %s -> %s | Bitrate: %d -> %d kbps%n",
                    qc.getOldQuality(), qc.getNewQuality(),
                    qc.getOldBitrate(), qc.getNewBitrate());
        } else if (payload instanceof RebufferPayload rb) {
            System.out.printf(" Rebuffer | Buffer: %dms | Bandwidth: %d kbps%n",
                    rb.getBufferLength(), rb.getBandwidthEstimate());
            if (rb.getRebufferDuration() != null) {
                System.out.printf(" Rebuffer Duration: %dms%n", rb.getRebufferDuration());
            }
        }
    }

    private static String formatMs(long ms) {
        long seconds = ms / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        return String.format("%02d:%02d:%02d", hours, minutes % 60, seconds % 60);
    }
}
