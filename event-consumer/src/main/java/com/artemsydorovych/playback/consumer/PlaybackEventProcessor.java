package com.artemsydorovych.playback.consumer;

import com.artemsydorovych.playback.avro.PlaybackEvent;
import com.artemsydorovych.playback.config.KafkaConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
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
 * Processes playback events from Kafka using Avro deserialization with Schema Registry.
 * <p>
 * This processor consumes events from the playback event stream and delegates
 * processing to a provided handler. It supports graceful shutdown and tracks
 * processing metrics.
 */
public class PlaybackEventProcessor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PlaybackEventProcessor.class);

    private final KafkaConsumer<String, PlaybackEvent> consumer;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    /**
     * Create a processor with default configuration.
     */
    public PlaybackEventProcessor() {
        this(KafkaConfig.BOOTSTRAP_SERVERS, KafkaConfig.SCHEMA_REGISTRY_URL,
                KafkaConfig.CONSUMER_GROUP_CONSOLE, KafkaConfig.TOPIC_PLAYBACK_EVENTS);
    }

    /**
     * Create a processor with custom configuration.
     */
    public PlaybackEventProcessor(String bootstrapServers, String schemaRegistryUrl,
                                  String groupId, String topic) {
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(createConfig(bootstrapServers, schemaRegistryUrl, groupId));
        log.info("PlaybackEventProcessor initialized - topic: '{}', group: '{}'", topic, groupId);
    }

    private Properties createConfig(String bootstrapServers, String schemaRegistryUrl, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        // Schema Registry configuration
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        // Consumer settings
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConfig.DEFAULT_AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, KafkaConfig.DEFAULT_AUTO_COMMIT_INTERVAL_MS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "playback-event-processor");

        return props;
    }

    /**
     * Start processing events with the provided handler.
     * <p>
     * This method blocks until {@link #stop()} is called or an error occurs.
     *
     * @param eventHandler Handler to process each event
     */
    public void process(Consumer<PlaybackEvent> eventHandler) {
        running.set(true);
        consumer.subscribe(Collections.singletonList(topic));
        log.info("Started processing events from topic '{}'. Waiting for events...", topic);

        try {
            while (running.get()) {
                ConsumerRecords<String, PlaybackEvent> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, PlaybackEvent> record : records) {
                    try {
                        PlaybackEvent event = record.value();
                        eventHandler.accept(event);
                        processedCount.incrementAndGet();
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        log.error("Failed to process event at partition {} offset {}: {}",
                                record.partition(), record.offset(), e.getMessage());
                    }
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                throw e;
            }
            // Expected during shutdown
        } finally {
            log.info("Stopped processing. Processed: {}, Errors: {}",
                    processedCount.get(), errorCount.get());
        }
    }

    /**
     * Signal the processor to stop.
     */
    public void stop() {
        running.set(false);
        consumer.wakeup();
    }

    /**
     * Check if the processor is currently running.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Get the total number of successfully processed events.
     */
    public long getProcessedCount() {
        return processedCount.get();
    }

    /**
     * Get the total number of processing errors.
     */
    public long getErrorCount() {
        return errorCount.get();
    }

    @Override
    public void close() {
        stop();
        consumer.close();
        log.info("PlaybackEventProcessor closed");
    }
}
