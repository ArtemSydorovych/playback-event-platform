package com.artemsydorovych.playback.generator;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main application to generate and publish playback events to Kafka.
 * <p>
 * Usage:
 *   gradlew :event-generator:run
 *   gradlew :event-generator:run --args="--count 50 --rate 100"
 *   gradlew :event-generator:run --args="--infinite --rate 50"
 *   gradlew :event-generator:run --args="--stress --threads 4"
 * <p>
 * Options:
 *   --count N     Number of events to generate (default: 100)
 *   --rate N      Events per second (default: 10)
 *   --delay N     Delay between events in ms (alternative to --rate)
 *   --infinite    Run forever until interrupted (Ctrl+C)
 *   --stress      Maximum speed mode, no delays (implies --infinite)
 *   --threads N   Number of parallel threads for stress test (default: 4)
 *   --batch N     Batch size for async sends before flush (default: 1000)
 */
public class EventGeneratorApp {

    private static final Logger log = LoggerFactory.getLogger(EventGeneratorApp.class);

    private static final int DEFAULT_EVENT_COUNT = 100;
    private static final int DEFAULT_RATE = 10;
    private static final int DEFAULT_THREADS = 4;
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final AtomicLong totalPublished = new AtomicLong(0);
    private static final AtomicLong totalFailed = new AtomicLong(0);

    public static void main(String[] args) {
        int eventCount = getIntArg(args, "--count", DEFAULT_EVENT_COUNT);
        int rate = getIntArg(args, "--rate", DEFAULT_RATE);
        int delayMs = getIntArg(args, "--delay", -1);
        boolean infinite = hasFlag(args, "--infinite");
        boolean stress = hasFlag(args, "--stress");
        int threads = getIntArg(args, "--threads", DEFAULT_THREADS);
        int batchSize = getIntArg(args, "--batch", DEFAULT_BATCH_SIZE);

        // Stress mode implies infinite and no delay
        if (stress) {
            infinite = true;
            delayMs = 0;
        } else if (delayMs < 0) {
            delayMs = rate > 0 ? 1000 / rate : 0;
        }

        // Register shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping generator...");
            running.set(false);
        }));

        log.info("=== Playback Event Generator ===");
        log.info("Bootstrap: {}", KafkaConfig.BOOTSTRAP_SERVERS);
        log.info("Schema Registry: {}", KafkaConfig.SCHEMA_REGISTRY_URL);
        log.info("Topic: {}", KafkaConfig.TOPIC_PLAYBACK_EVENTS);

        if (stress) {
            log.info("Mode: STRESS TEST (press Ctrl+C to stop)");
            log.info("Threads: {}, Batch size: {}", threads, batchSize);
            runStressTest(threads, batchSize);
        } else if (infinite) {
            log.info("Mode: INFINITE (press Ctrl+C to stop)");
            log.info("Rate: ~{} events/sec", rate);
            runSingleThreaded(eventCount, delayMs, true);
        } else {
            log.info("Events: {}, Rate: ~{} events/sec", eventCount, rate);
            runSingleThreaded(eventCount, delayMs, false);
        }
    }

    private static void runSingleThreaded(int eventCount, int delayMs, boolean infinite) {
        PlaybackActivityGenerator generator = new PlaybackActivityGenerator();

        try (PlaybackEventPublisher publisher = new PlaybackEventPublisher()) {
            long startTime = System.currentTimeMillis();
            long count = 0;
            long lastLogTime = startTime;

            while (running.get() && (infinite || count < eventCount)) {
                PlaybackEvent event = generator.generate();
                publisher.publish(event);
                count++;

                long now = System.currentTimeMillis();
                if (count % 10 == 0 || (infinite && now - lastLogTime > 5000)) {
                    if (infinite) {
                        double elapsed = (now - startTime) / 1000.0;
                        double actualRate = count / elapsed;
                        log.info("Published {} events ({} events/sec)...", count, String.format("%.1f", actualRate));
                    } else {
                        log.info("Published {} events...", count);
                    }
                    lastLogTime = now;
                }

                if (delayMs > 0) {
                    Thread.sleep(delayMs);
                }
            }

            publisher.flush();
            long elapsed = System.currentTimeMillis() - startTime;
            double actualRate = elapsed > 0 ? count / (elapsed / 1000.0) : 0;
            log.info("Completed! Published: {}, Failed: {}, Duration: {}s, Rate: {}/sec",
                    publisher.getPublishedCount(), publisher.getFailedCount(),
                    elapsed / 1000, String.format("%.1f", actualRate));

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Generator interrupted");
        } catch (Exception e) {
            log.error("Error: {}", e.getMessage(), e);
        }
    }

    private static void runStressTest(int numThreads, int batchSize) {
        log.info("Starting stress test with {} threads...", numThreads);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        long startTime = System.currentTimeMillis();

        // Start progress reporter thread
        Thread reporter = new Thread(() -> {
            long lastCount = 0;
            long lastTime = System.currentTimeMillis();

            while (running.get()) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    break;
                }

                long currentCount = totalPublished.get();
                long currentTime = System.currentTimeMillis();
                long elapsed = currentTime - startTime;
                long intervalCount = currentCount - lastCount;
                long intervalTime = currentTime - lastTime;

                double overallRate = elapsed > 0 ? currentCount / (elapsed / 1000.0) : 0;
                double currentRate = intervalTime > 0 ? intervalCount / (intervalTime / 1000.0) : 0;

                log.info("STRESS: {} total events | Current: {} events/sec | Average: {} events/sec",
                        currentCount,
                        String.format("%.0f", currentRate),
                        String.format("%.0f", overallRate));

                lastCount = currentCount;
                lastTime = currentTime;
            }
        });
        reporter.setDaemon(true);
        reporter.start();

        // Start worker threads
        List<PlaybackEventPublisher> publishers = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            PlaybackEventPublisher publisher = new PlaybackEventPublisher();
            publishers.add(publisher);

            executor.submit(() -> {
                PlaybackActivityGenerator generator = new PlaybackActivityGenerator();
                int localBatchCount = 0;

                log.info("Worker {} started", threadId);

                while (running.get()) {
                    try {
                        PlaybackEvent event = generator.generate();
                        publisher.publish(event);
                        totalPublished.incrementAndGet();
                        localBatchCount++;

                        // Periodic flush to prevent memory buildup
                        if (localBatchCount >= batchSize) {
                            publisher.flush();
                            localBatchCount = 0;
                        }
                    } catch (Exception e) {
                        totalFailed.incrementAndGet();
                        if (running.get()) {
                            log.warn("Worker {} error: {}", threadId, e.getMessage());
                        }
                    }
                }

                log.info("Worker {} stopping...", threadId);
            });
        }

        // Wait for shutdown signal
        try {
            while (running.get()) {
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Shutdown
        log.info("Shutting down workers...");
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        // Flush and close all publishers
        for (PlaybackEventPublisher publisher : publishers) {
            try {
                publisher.flush();
                publisher.close();
            } catch (Exception e) {
                log.warn("Error closing publisher: {}", e.getMessage());
            }
        }

        reporter.interrupt();

        long elapsed = System.currentTimeMillis() - startTime;
        long total = totalPublished.get();
        double avgRate = elapsed > 0 ? total / (elapsed / 1000.0) : 0;

        log.info("=== STRESS TEST COMPLETE ===");
        log.info("Total published: {}", total);
        log.info("Total failed: {}", totalFailed.get());
        log.info("Duration: {}s", elapsed / 1000);
        log.info("Average rate: {} events/sec", String.format("%.0f", avgRate));
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

    private static boolean hasFlag(String[] args, String flag) {
        for (String arg : args) {
            if (arg.equals(flag)) {
                return true;
            }
        }
        return false;
    }
}
