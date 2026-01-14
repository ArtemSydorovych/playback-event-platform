package com.netflix.playback.consumer;

import com.netflix.playback.model.PlaybackEvent;
import com.netflix.playback.serialization.JsonSerializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Simple Kafka consumer for PlaybackEvents.
 */
public class SimpleEventConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SimpleEventConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong consumedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public SimpleEventConsumer(String bootstrapServers, String groupId, String topic) {
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(createConfig(bootstrapServers, groupId));
        log.info("Consumer created for topic '{}', group '{}'", topic, groupId);
    }

    private Properties createConfig(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "event-consumer");
        return props;
    }

    /**
     * Start consuming events and process them with the given handler.
     */
    public void consume(Consumer<PlaybackEvent> eventHandler) {
        running.set(true);
        consumer.subscribe(Collections.singletonList(topic));
        log.info("Subscribed to topic '{}'. Waiting for events...", topic);

        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        PlaybackEvent event = JsonSerializer.deserialize(record.value());
                        eventHandler.accept(event);
                        consumedCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        log.error("Failed to process record at offset {}: {}",
                            record.offset(), e.getMessage());
                    }
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
            // Expected during shutdown
        } finally {
            log.info("Consumer stopped. Consumed: {}, Errors: {}", consumedCount.get(), errorCount.get());
        }
    }

    /**
     * Signal the consumer to stop.
     */
    public void stop() {
        running.set(false);
        consumer.wakeup();
    }

    public long getConsumedCount() {
        return consumedCount.get();
    }

    public long getErrorCount() {
        return errorCount.get();
    }

    @Override
    public void close() {
        stop();
        consumer.close();
    }
}
