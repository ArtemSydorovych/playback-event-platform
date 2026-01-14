package com.netflix.playback.consumer;

import com.netflix.playback.model.PlaybackEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application that consumes playback events and prints them to console.
 */
public class ConsoleConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(ConsoleConsumerApp.class);

    private static final String BOOTSTRAP_SERVERS = System.getenv()
        .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String GROUP_ID = System.getenv()
        .getOrDefault("KAFKA_GROUP_ID", "console-printer-group");
    private static final String TOPIC = System.getenv()
        .getOrDefault("KAFKA_TOPIC", "playback-events");

    public static void main(String[] args) {
        log.info("Starting Console Consumer");
        log.info("Bootstrap servers: {}", BOOTSTRAP_SERVERS);
        log.info("Group ID: {}", GROUP_ID);
        log.info("Topic: {}", TOPIC);

        try (SimpleEventConsumer consumer = new SimpleEventConsumer(BOOTSTRAP_SERVERS, GROUP_ID, TOPIC)) {

            // Add shutdown hook for graceful stop
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received...");
                consumer.stop();
            }));

            // Start consuming and print events
            consumer.consume(ConsoleConsumerApp::printEvent);

        } catch (Exception e) {
            log.error("Error: {}", e.getMessage(), e);
        }
    }

    private static void printEvent(PlaybackEvent event) {
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.printf(" Event: %-15s │ User: %-10s │ Content: %s%n",
            event.getEventType(),
            event.getUserId(),
            event.getContentId());
        System.out.println("───────────────────────────────────────────────────────────");
        System.out.printf(" Position: %s │ Progress: %.1f%%%n",
            event.getFormattedPosition(),
            event.getCompletionPercentage());
        System.out.printf(" Device: %-12s │ Country: %s%n",
            event.getDeviceInfo() != null ? event.getDeviceInfo().getDeviceType() : "N/A",
            event.getDeviceInfo() != null ? event.getDeviceInfo().getCountry() : "N/A");
        System.out.printf(" Session: %s │ Time: %s%n",
            event.getSessionId(),
            event.getTimestamp());

        if (event.getPayload() != null) {
            System.out.println("───────────────────────────────────────────────────────────");
            if (event.getPayload().getSeek() != null) {
                var seek = event.getPayload().getSeek();
                System.out.printf(" Seek: %s -> %s (%s)%n",
                    formatMs(seek.getFromPosition()),
                    formatMs(seek.getToPosition()),
                    seek.isForwardSeek() ? "forward" : "backward");
            }
            if (event.getPayload().getProgress() != null) {
                var progress = event.getPayload().getProgress();
                System.out.printf(" Quality: %s @ %d kbps │ Buffer: %dms%n",
                    progress.getCurrentQuality(),
                    progress.getCurrentBitrate(),
                    progress.getBufferLength());
            }
            if (event.getPayload().getPlaybackEnd() != null) {
                var end = event.getPayload().getPlaybackEnd();
                System.out.printf(" End Reason: %s │ Watched: %s (%.1f%%)%n",
                    end.getReason(),
                    formatMs(end.getWatchDuration()),
                    end.getCompletionPercentage());
            }
            if (event.getPayload().getQualityChange() != null) {
                var qc = event.getPayload().getQualityChange();
                System.out.printf(" Quality: %s -> %s │ Reason: %s%n",
                    qc.getOldQuality(), qc.getNewQuality(), qc.getReason());
            }
        }
        System.out.println("═══════════════════════════════════════════════════════════");
    }

    private static String formatMs(long ms) {
        long seconds = ms / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        return String.format("%02d:%02d:%02d", hours, minutes % 60, seconds % 60);
    }
}
